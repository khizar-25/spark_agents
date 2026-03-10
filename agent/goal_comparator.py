# agent/goal_comparator.py
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List
from config.goal_state import SparkGoalState
from agent.state_detector import SparkPipelineState

logger = logging.getLogger(__name__)

@dataclass
class ComparisonResult:
    completion_score: float
    is_complete: bool
    met_criteria: List[str] = field(default_factory=list)
    unmet_criteria: List[str] = field(default_factory=list)
    blocking_issues: List[str] = field(default_factory=list)
    next_recommended_step: str = ""

    def to_dict(self):
        return {
            "completion_score": round(self.completion_score, 3),
            "is_complete": self.is_complete,
            "met_criteria": self.met_criteria,
            "unmet_criteria": self.unmet_criteria,
            "blocking_issues": self.blocking_issues,
            "next_recommended_step": self.next_recommended_step,
        }

class GoalComparator:
    GOAL_THRESHOLD: float = 0.80

    def compare(self, goal: SparkGoalState, state: SparkPipelineState) -> ComparisonResult:
        checks = []
        checks.append(("source_accessible",   state.source_exists,       True))
        checks.append(("spark_initialized",    state.spark_initialized,   True))
        checks.append(("schema_inferred",      state.schema_inferred,     True))

        # Transformations: check count, not exact string match
        required = goal.transformations or []
        applied  = state.transformations_applied or []
        trans_done = len(applied) >= max(1, len(required))
        checks.append(("transformations_applied", trans_done, True))

        if goal.dq_rules:
            checks.append(("dq_ran",    state.dq_ran,    True))
            checks.append(("dq_passed", state.dq_passed, True))

        checks.append(("output_written", state.output_written, True))

        if state.output_written:
            checks.append(("lineage_captured", state.lineage_captured, False))

        met      = [n for n, p, _ in checks if p]
        unmet    = [n for n, p, _ in checks if not p]
        blocking = [n for n, p, b in checks if not p and b]

        total = len(checks)
        score = len(met) / total if total > 0 else 0.0
        # Complete when output is written and DQ passed (or no DQ required)
        is_complete = state.output_written and (state.dq_passed or not goal.dq_rules)

        result = ComparisonResult(
            completion_score=score,
            is_complete=is_complete,
            met_criteria=met,
            unmet_criteria=unmet,
            blocking_issues=blocking,
            next_recommended_step=self._recommend(unmet, blocking, state),
        )
        logger.info(f"[GoalComparator] Score={score:.0%} | Met={len(met)}/{total} | Blocking={len(blocking)} | Complete={is_complete}")
        return result

    def _recommend(self, unmet, blocking, state):
        if "source_accessible"    in blocking: return "verify_source_path"
        if "spark_initialized"    in blocking: return "initialize_spark"
        if "schema_inferred"      in blocking: return "infer_schema"
        if "transformations_applied" in blocking: return "apply_transformations"
        if "dq_ran"               in blocking: return "run_data_quality"
        if "dq_passed"            in blocking: return "fix_dq_failures"
        if "output_written"       in blocking: return "write_output"
        if "lineage_captured"     in unmet:    return "generate_lineage"
        return "goal_complete"
