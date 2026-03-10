# config/goal_state.py
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from enum import Enum

class PipelineGoalType(Enum):
    ETL_BATCH        = "etl_batch"
    STREAM_INGEST    = "stream_ingest"
    DATA_QUALITY     = "data_quality"
    SCHEMA_MIGRATION = "schema_migration"
    AGGREGATION      = "aggregation"
    CUSTOM           = "custom"

@dataclass
class SparkGoalState:
    goal_type: PipelineGoalType
    description: str
    source_path: str
    target_path: str
    input_format: str = "csv"
    output_format: str = "parquet"
    write_mode: str = "append"
    partition_cols: List[str] = field(default_factory=list)
    transformations: List[str] = field(default_factory=list)
    dq_rules: Dict[str, Any] = field(default_factory=dict)
    expected_row_count_min: Optional[int] = None
    expected_columns: Optional[List[str]] = None
    sla_seconds: Optional[int] = None

    def to_dict(self):
        return {
            "goal_type": self.goal_type.value,
            "description": self.description,
            "source_path": self.source_path,
            "target_path": self.target_path,
            "input_format": self.input_format,
            "output_format": self.output_format,
            "write_mode": self.write_mode,
            "partition_cols": self.partition_cols,
            "transformations": self.transformations,
            "dq_rules": self.dq_rules,
        }

def etl_goal(source: str, target: str, **kwargs) -> SparkGoalState:
    return SparkGoalState(
        goal_type=PipelineGoalType.ETL_BATCH,
        description=f"Read from {source}, deduplicate, add audit columns, write to {target} in Delta format",
        source_path=source, target_path=target,
        input_format="csv", output_format="parquet",
        partition_cols=["date"],
        transformations=["deduplicate", "cast_types", "handle_nulls", "add_audit_cols"],
        dq_rules={"null_threshold": 0.01, "dupe_check": True},
        **kwargs,
    )

def streaming_goal(broker: str, topic: str, target: str, **kwargs) -> SparkGoalState:
    return SparkGoalState(
        goal_type=PipelineGoalType.STREAM_INGEST,
        description=f"Consume {topic} from Kafka {broker}, write to {target}",
        source_path=f"{broker}/{topic}", target_path=target,
        input_format="kafka", output_format="parquet",
        write_mode="append", partition_cols=["hour"], **kwargs,
    )

def dq_goal(source: str, report_path: str, **kwargs) -> SparkGoalState:
    return SparkGoalState(
        goal_type=PipelineGoalType.DATA_QUALITY,
        description=f"Run full DQ suite on {source}, publish report to {report_path}",
        source_path=source, target_path=report_path,
        dq_rules={"null_threshold": 0.005, "dupe_check": True, "freshness_hours": 2},
        **kwargs,
    )
