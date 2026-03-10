# agent/agent.py
import logging, time
from dataclasses import dataclass, field, asdict
from typing import Any, Callable, Dict, List, Optional

from agent.llm_reasoner   import LLMReasoner
from agent.state_detector import StateDetector, SparkPipelineState
from agent.goal_comparator import GoalComparator, ComparisonResult
from agent.tool_validator  import ToolValidator
from config.goal_state     import SparkGoalState
from config.settings       import MAX_ITERATIONS, TOOL_RETRY_LIMIT
from tools.spark_tools     import TOOL_REGISTRY

logger = logging.getLogger(__name__)

DONE_SIGNALS = {"goal_complete","none","null","finished","complete","done","no_tool","goal_achieved"}

@dataclass
class AgentRunResult:
    success: bool
    goal_description: str
    iterations_used: int
    final_completion_score: float
    final_state: Dict[str, Any]
    action_history: List[Dict[str, Any]]
    metrics: Dict[str, Any]
    error: Optional[str] = None
    duration_seconds: float = 0.0
    def to_dict(self): return asdict(self)

class SparkAgent:
    def __init__(self, on_step: Optional[Callable[[Dict], None]] = None):
        self.state_detector  = StateDetector()
        self.goal_comparator = GoalComparator()
        self.tool_validator  = ToolValidator()
        self.reasoner        = LLMReasoner(tool_registry=TOOL_REGISTRY)
        self.on_step         = on_step
        self._action_history: List[str] = []
        self._full_history:   List[Dict] = []
        self._error_streak:   int = 0
        self._start_time:     float = 0.0

    def run(self, goal: SparkGoalState) -> AgentRunResult:
        self._start_time = time.time()
        self.state_detector.reset()
        self.reasoner.reset_history()
        self._action_history = []
        self._full_history   = []
        self._error_streak   = 0

        logger.info(f"\n{'='*60}")
        logger.info(f"[SparkAgent] Starting: {goal.description}")
        logger.info(f"{'='*60}")

        state = self.state_detector.observe(source_path=goal.source_path)

        for iteration in range(1, MAX_ITERATIONS + 1):
            logger.info(f"\n[SparkAgent] ── Iteration {iteration}/{MAX_ITERATIONS} ──")

            comparison = self.goal_comparator.compare(goal, state)
            logger.info(f"[SparkAgent] Goal progress: {comparison.completion_score:.0%}")
            self._emit_step(iteration, state, comparison)

            # ── SUCCESS check ──────────────────────────────────
            if comparison.is_complete:
                logger.info(f"[SparkAgent] ✅ Goal COMPLETE at iteration {iteration}!")
                return self._build_result(True, goal, iteration, comparison, state)

            # ── LLM decision ───────────────────────────────────
            try:
                decision = self.reasoner.decide_next_action(
                    current_state=state.to_dict(),
                    goal_description=goal.description,
                    action_history=self._action_history,
                    iteration=iteration,
                )
            except Exception as e:
                logger.error(f"[SparkAgent] LLM failed: {e}")
                state.add_error(f"LLM error: {e}")
                self._error_streak += 1
                if self._error_streak >= TOOL_RETRY_LIMIT:
                    return self._build_result(False, goal, iteration, comparison, state, error=str(e))
                continue

            tool_name  = str(decision.get("tool_name") or "").strip().lower().replace(" ","_")
            tool_args  = decision.get("tool_args", {}) or {}
            reasoning  = decision.get("reasoning", "")
            confidence = decision.get("confidence", 0.5)

            # ── LLM signals done ───────────────────────────────
            if tool_name in DONE_SIGNALS:
                logger.info(f"[SparkAgent] LLM signals completion (tool={tool_name})")
                if state.output_written:
                    logger.info(f"[SparkAgent] ✅ Output written — SUCCESS!")
                    return self._build_result(True, goal, iteration, comparison, state)
                else:
                    # Force write_output
                    logger.info(f"[SparkAgent] Forcing write_output since output not written yet...")
                    tool_name = "write_output"
                    tool_args = {
                        "target_path": goal.target_path,
                        "format": goal.output_format,
                        "mode": goal.write_mode,
                    }
                    if goal.partition_cols:
                        tool_args["partition_cols"] = goal.partition_cols

            logger.info(f"[SparkAgent] Decision: {tool_name} (confidence={confidence:.2f})")
            logger.info(f"[SparkAgent] Reasoning: {reasoning[:120]}")

            # ── Validate ───────────────────────────────────────
            is_valid, reason = self.tool_validator.validate(tool_name, tool_args, state, TOOL_REGISTRY)
            if not is_valid:
                logger.warning(f"[SparkAgent] INVALID: {reason}")
                state.add_error(f"Validation failed for {tool_name}: {reason}")
                self._action_history.append(f"INVALID: {tool_name} — {reason}")
                self._error_streak += 1
                continue

            # ── Execute ────────────────────────────────────────
            result = self._execute(tool_name, tool_args)
            self._full_history.append({"iteration":iteration,"tool":tool_name,"args":tool_args,
                                        "result_status":result.get("status"),"reasoning":reasoning[:200],"confidence":confidence})
            self._action_history.append(f"[{iteration}] {tool_name} → {result.get('status')}")

            if result.get("status") == "error":
                self._error_streak += 1
                state.add_error(f"{tool_name} failed: {result.get('message','')}")
            else:
                self._error_streak = 0
                self._update_state(tool_name, tool_args, result, state)

            state = self.state_detector.observe(source_path=goal.source_path)

            if self._error_streak >= TOOL_RETRY_LIMIT:
                logger.warning(f"[SparkAgent] {TOOL_RETRY_LIMIT} errors in a row. Diagnosing...")
                self.reasoner.diagnose(state.to_dict(), state.errors, goal.description)
                self._error_streak = 0

        # Max iterations — if output written treat as success
        final = self.goal_comparator.compare(goal, state)
        success = final.is_complete or state.output_written
        return self._build_result(success, goal, MAX_ITERATIONS, final, state,
                                   error=None if success else "Max iterations reached")

    def _execute(self, tool_name, tool_args):
        meta = TOOL_REGISTRY.get(tool_name)
        if not meta: return {"status":"error","message":f"Tool {tool_name} not found"}
        try:
            logger.info(f"[SparkAgent] Executing: {tool_name}({list(tool_args.keys())})")
            result = meta["fn"](**tool_args)
            logger.info(f"[SparkAgent] Result: {str(result)[:120]}")
            return result
        except Exception as e:
            logger.error(f"[SparkAgent] {tool_name} raised: {e}")
            return {"status":"error","message":str(e)}

    def _update_state(self, tool_name, tool_args, result, state):
        sd = self.state_detector
        if tool_name in ("initialize_spark","auto_configure_and_start"):
            sd._state.spark_initialized = True
            sd._state.spark_master = result.get("master","local[*]")
            if result.get("auto_configured"):
                sd._state.spark_config_tuned = True
        elif tool_name == "auto_configure_spark":
            sd._state.spark_config_tuned = True
        elif tool_name == "infer_schema":
            sd._state.schema_inferred = True
            sd._state.schema_columns = result.get("columns",[])
            sd.mark_pipeline_stage("transforming")
        elif tool_name == "apply_transformations":
            ops = result.get("operations_applied",[])
            for op in ops:
                name = op.get("operation",str(op)) if isinstance(op,dict) else str(op)
                sd.record_transformation(name)
            sd._state.rows_read = result.get("row_count",0)
        elif tool_name == "run_data_quality":
            sd.record_dq_result(
                passed=result.get("passed",False),
                score=result.get("dq_score",0.0),
                failures=result.get("failures",[]),
            )
        elif tool_name == "write_output":
            sd.record_output(
                path=tool_args.get("target_path",""),
                fmt=tool_args.get("format","delta"),
                rows=result.get("rows_written",0),
                size_bytes=result.get("size_bytes",0),
            )
            sd.mark_pipeline_stage("done")
        elif tool_name == "generate_lineage":
            sd.record_lineage(result.get("lineage",{}).get("node_count",0))
        elif tool_name == "repartition":
            sd._state.partitions = result.get("new_partition_count",0)

    def _emit_step(self, iteration, state, comparison):
        if self.on_step:
            self.on_step({"iteration":iteration,"completion_score":comparison.completion_score,
                          "pipeline_stage":state.pipeline_stage,"next_step":comparison.next_recommended_step})

    def _build_result(self, success, goal, iterations, comparison, state, error=None):
        duration = time.time() - self._start_time
        return AgentRunResult(
            success=success, goal_description=goal.description,
            iterations_used=iterations, final_completion_score=comparison.completion_score,
            final_state=state.to_dict(), action_history=self._full_history,
            metrics={"rows_read":state.rows_read,"rows_written":state.rows_written,
                     "dq_score":state.dq_score,"partitions":state.partitions,
                     "transformations":state.transformations_applied,
                     "duration_seconds":round(duration,2),
                     "elapsed_str":f"{int(duration//60)}m {int(duration%60)}s"},
            duration_seconds=round(duration,2), error=error,
        )
