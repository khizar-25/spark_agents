# config/settings.py
import os
from pathlib import Path
from typing import Optional

# Load .env if exists
env_file = Path(__file__).parent.parent / ".env"
if env_file.exists():
    for line in env_file.read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

OPENROUTER_API_KEY: str = os.getenv("OPENROUTER_API_KEY", "")
LLM_MODEL: str          = os.getenv("LLM_MODEL", "openrouter/auto")
LLM_MAX_TOKENS: int     = 4096
LLM_TEMPERATURE: float  = 0.2

SPARK_MASTER: str            = os.getenv("SPARK_MASTER", "local[*]")
SPARK_APP_NAME: str          = "SparkAgent"
SPARK_EXECUTOR_MEMORY: str   = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")
SPARK_EXECUTOR_CORES: int    = int(os.getenv("SPARK_EXECUTOR_CORES", "2"))
SPARK_DRIVER_MEMORY: str     = os.getenv("SPARK_DRIVER_MEMORY", "1g")
SPARK_SHUFFLE_PARTITIONS:int = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "200"))
SPARK_ADAPTIVE_ENABLED: bool = True

DEFAULT_INPUT_FORMAT: str  = "csv"
DEFAULT_OUTPUT_FORMAT: str = "delta"
DEFAULT_WRITE_MODE: str    = "append"
CHECKPOINT_DIR: str        = os.getenv("CHECKPOINT_DIR", "/tmp/spark_agent_checkpoints")

MAX_ITERATIONS: int              = 15
GOAL_SIMILARITY_THRESHOLD: float = 0.85
TOOL_RETRY_LIMIT: int            = 3

ENABLE_DATA_QUALITY: bool    = True
ENABLE_LINEAGE_TRACKING: bool = True
ENABLE_AUTO_OPTIMIZE: bool   = True

SLACK_WEBHOOK_URL: Optional[str] = os.getenv("SLACK_WEBHOOK_URL")
ALERT_ON_FAILURE: bool           = True

API_HOST: str  = "0.0.0.0"
API_PORT: int  = 8000
API_DEBUG: bool = os.getenv("ENV", "dev") == "dev"
