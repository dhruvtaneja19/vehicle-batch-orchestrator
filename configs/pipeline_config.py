"""
Project Configuration

All configurable values should live here.
This makes the project easier to maintain.
"""

from pathlib import Path
from airflow.sdk import Variable

# Project Root
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Input and Output folders
INPUT_FOLDER = PROJECT_ROOT / "data" / "input"
OUTPUT_FOLDER = PROJECT_ROOT / "data" / "output"

# Default input file
DEFAULT_INPUT_FILE = INPUT_FOLDER / "vehicle_data.csv"

# S3 Configuration
S3_BUCKET = Variable.get(
    "S3_BUCKET",
    default_var="vehicle-batch-airflow"
)
S3_INPUT_KEY = "input/vehicle_data.csv"
S3_OUTPUT_KEY = "output/final_vehicle_report.csv"
S3_FAILED_KEY = "failed/failed_records.csv"

# Pipeline Settings
PIPELINE_NAME = "Vehicle Batch Orchestrator"

DEFAULT_CHUNK_SIZE = 2