# agent/state_detector.py
import logging, os, time
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

@dataclass
class SparkPipelineState:
    source_path: str = ""
    source_format: str = ""
    source_exists: bool = False
    source_file_count: int = 0
    source_size_bytes: int = 0
    schema_inferred: bool = False
    schema_columns: List[str] = field(default_factory=list)
    schema_drift_detected: bool = False
    schema_new_columns: List[str] = field(default_factory=list)
    spark_initialized: bool = False
    spark_master: str = ""
    spark_config_tuned: bool = False
    active_jobs: int = 0
    pipeline_stage: str = "idle"
    transformations_applied: List[str] = field(default_factory=list)
    rows_read: int = 0
    rows_written: int = 0
    partitions: int = 0
    dq_ran: bool = False
    dq_passed: bool = False
    dq_score: float = 0.0
    dq_failures: List[str] = field(default_factory=list)
    output_written: bool = False
    output_path: str = ""
    output_format: str = ""
    output_size_bytes: int = 0
    lineage_captured: bool = False
    lineage_nodes: int = 0
    elapsed_seconds: float = 0.0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    last_updated: float = field(default_factory=time.time)

    def to_dict(self): return asdict(self)
    def add_error(self, msg):
        logger.error(f"[State] {msg}")
        self.errors.append(msg)
    def add_warning(self, msg):
        logger.warning(f"[State] {msg}")
        self.warnings.append(msg)

class StateDetector:
    def __init__(self):
        self._state = SparkPipelineState()
        self._start_time = time.time()

    def observe(self, source_path=None, spark=None, df=None, extra=None):
        self._state.elapsed_seconds = time.time() - self._start_time
        self._state.last_updated = time.time()
        if source_path:
            self._observe_source(source_path)
        if spark:
            self._observe_spark(spark)
        if df is not None:
            self._observe_dataframe(df)
        if extra:
            self._apply_extra(extra)
        return self._state

    def _observe_source(self, path):
        self._state.source_path = path
        if path.startswith(("s3://","gs://","abfs://","kafka://")):
            self._state.source_exists = True
        elif os.path.exists(path):
            self._state.source_exists = True
            self._state.source_size_bytes = self._dir_size(path)
            self._state.source_file_count = self._count_files(path)
            for fmt in ["parquet","delta","json","csv","orc","avro"]:
                if fmt in path.lower():
                    self._state.source_format = fmt
                    break
            if not self._state.source_format:
                self._state.source_format = "csv"
        else:
            self._state.source_exists = False

    def _dir_size(self, path):
        total = 0
        try:
            for dp, _, files in os.walk(path):
                for f in files:
                    total += os.path.getsize(os.path.join(dp, f))
        except: pass
        return total

    def _count_files(self, path):
        try:
            if os.path.isfile(path): return 1
            return sum(len(f) for _, _, f in os.walk(path))
        except: return 0

    def _observe_spark(self, spark):
        try:
            self._state.spark_initialized = True
            self._state.spark_master = spark.conf.get("spark.master","unknown")
            self._state.spark_config_tuned = spark.conf.get("spark.sql.adaptive.enabled","false").lower() == "true"
        except Exception as e:
            self._state.add_warning(f"Cannot observe Spark: {e}")

    def _observe_dataframe(self, df):
        try:
            self._state.schema_inferred = True
            self._state.schema_columns = [f.name for f in df.schema.fields]
            self._state.partitions = df.rdd.getNumPartitions()
        except Exception as e:
            self._state.add_warning(f"Cannot observe DataFrame: {e}")

    def _apply_extra(self, extra):
        for k, v in extra.items():
            if hasattr(self._state, k):
                setattr(self._state, k, v)

    def mark_pipeline_stage(self, stage):
        self._state.pipeline_stage = stage

    def record_transformation(self, name):
        if name not in self._state.transformations_applied:
            self._state.transformations_applied.append(name)

    def record_dq_result(self, passed, score, failures):
        self._state.dq_ran = True
        self._state.dq_passed = passed
        self._state.dq_score = score
        self._state.dq_failures = failures

    def record_output(self, path, fmt, rows, size_bytes=0):
        self._state.output_written = True
        self._state.output_path = path
        self._state.output_format = fmt
        self._state.rows_written = rows
        self._state.output_size_bytes = size_bytes

    def record_lineage(self, node_count):
        self._state.lineage_captured = True
        self._state.lineage_nodes = node_count

    def get_state(self): return self._state

    def reset(self):
        self._state = SparkPipelineState()
        self._start_time = time.time()
