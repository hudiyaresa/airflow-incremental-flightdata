import logging
from pathlib import Path
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.exceptions import AirflowException

QUERY_DIR = Path(__file__).resolve().parents[1] / "query"


class Transform:
    @staticmethod
    def build_operator(task_id: str, table_name: str, sql_dir: str = "final"):
        """
        Build a SQLExecuteQueryOperator to transform data for the given table.

        Args:
            task_id (str): Task ID for the operator.
            table_name (str): Name of the final table to transform.
            sql_dir (str): Folder under query/ that holds the SQL file.

        Returns:
            SQLExecuteQueryOperator: Airflow operator to run the transformation query.
        """
        if not (QUERY_DIR / sql_dir / f"{table_name}.sql").exists():
            raise AirflowException(f"[Transform] SQL file not found for table: {table_name}")

        logging.info(f"[Transform] Building transformation for: {table_name}")
        # Resolved through template_searchpath on the DAG, so no absolute container path
        return SQLExecuteQueryOperator(
            task_id=task_id,
            conn_id='warehouse_pacflight',
            sql=f"{sql_dir}/{table_name}.sql"
        )
