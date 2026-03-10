# agent/tool_validator.py
import logging
from typing import Any, Dict, List, Tuple
from agent.state_detector import SparkPipelineState

logger = logging.getLogger(__name__)

class ValidationError(Exception): pass

class ToolValidator:
    REQUIRES_SPARK  = {"infer_schema","apply_transformations","run_data_quality","write_output","repartition","generate_lineage"}
    REQUIRES_SCHEMA = {"apply_transformations","run_data_quality","write_output","generate_lineage"}
    REQUIRED_ARGS: Dict[str, List[str]] = {
        "initialize_spark":      ["master","app_name"],
        "infer_schema":          ["source_path","format"],
        "apply_transformations": ["operations"],
        "run_data_quality":      ["rules"],
        "write_output":          ["target_path","format","mode"],
        "alert_operator":        ["message","severity"],
        "repartition":           ["num_partitions"],
    }

    def validate(self, tool_name, tool_args, state, known_tools) -> Tuple[bool, str]:
        try:
            if tool_name == "goal_complete": return True, "ok"
            self._check_exists(tool_name, known_tools)
            self._check_args(tool_name, tool_args)
            self._check_spark(tool_name, state)
            self._check_schema(tool_name, state)
            self._check_paths(tool_args)
            return True, "ok"
        except ValidationError as e:
            logger.warning(f"[ToolValidator] INVALID: {tool_name} — {e}")
            return False, str(e)

    def _check_exists(self, name, registry):
        if name not in registry:
            raise ValidationError(f"Tool '{name}' not registered. Available: {list(registry.keys())}")

    def _check_args(self, name, args):
        missing = [r for r in self.REQUIRED_ARGS.get(name, []) if r not in args]
        if missing:
            raise ValidationError(f"Missing args for {name}: {missing}")

    def _check_spark(self, name, state):
        if name in self.REQUIRES_SPARK and not state.spark_initialized:
            raise ValidationError(f"'{name}' needs Spark initialized first.")

    def _check_schema(self, name, state):
        if name in self.REQUIRES_SCHEMA and not state.schema_inferred:
            raise ValidationError(f"'{name}' needs schema inferred first.")

    def _check_paths(self, args):
        for key in ["target_path","source_path","path"]:
            if args.get(key) in ["/","/etc","/usr","/bin"]:
                raise ValidationError(f"Dangerous path: {args[key]}")
