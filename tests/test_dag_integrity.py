import pytest
from airflow.models import DagBag

from conftest import DAGS_DIR


@pytest.fixture(scope="session")
def dag_bag():
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_no_import_errors(dag_bag):
    assert dag_bag.import_errors == {}


def test_expected_dags_exist(dag_bag):
    assert {"flights_data_pipeline", "test_pacflight_connections"} <= set(dag_bag.dag_ids)


def test_main_dag_is_safe_to_backfill(dag_bag):
    dag = dag_bag.get_dag("flights_data_pipeline")
    assert dag.catchup is True
    assert dag.max_active_runs == 1
    assert dag.default_args["on_failure_callback"] is not None


def test_task_groups_run_in_order(dag_bag):
    dag = dag_bag.get_dag("flights_data_pipeline")
    assert list(dag.task_group.children) == ["extract", "load", "transform"]


def test_every_transform_has_sql_file(dag_bag):
    dag = dag_bag.get_dag("flights_data_pipeline")
    query_dir = DAGS_DIR / "flights_data_pipeline" / "query" / "final"
    transform_ids = [t.task_id for t in dag.tasks if t.task_id.startswith("transform.")]
    assert transform_ids
    for task_id in transform_ids:
        table = task_id.split(".", 1)[1]
        assert (query_dir / f"{table}.sql").exists(), table
