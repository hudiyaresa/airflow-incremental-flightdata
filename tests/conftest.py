import os
import sys
from pathlib import Path

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"

# DAG files import their own packages (flights_data_pipeline, helper) the same way Airflow does
sys.path.insert(0, str(DAGS_DIR))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
