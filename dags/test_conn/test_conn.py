from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from datetime import datetime
import logging

@dag(
    dag_id="test_pacflight_connections",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["test", "connection"],
)
def test_connection_dag():

    @task
    def test_postgres_connection(connection_id: str):
        logging.info(f"⏳ Testing connection to `{connection_id}`...")
        try:
            pg_hook = PostgresHook(postgres_conn_id=connection_id)
            conn = pg_hook.get_conn()
            cursor = conn.cursor()
            cursor.execute("SELECT 1;")
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            logging.info(f"✅ Connection to `{connection_id}` successful. Query returned: {result}")
        except Exception as e:
            logging.error(f"❌ Connection to `{connection_id}` failed: {e}")
            raise AirflowException(f"Failed to connect to `{connection_id}`: {e}")

    # Jalankan task untuk kedua koneksi
    test_postgres_connection.override(task_id="test_connection_pacflight_db")("pacflight_db")
    test_postgres_connection.override(task_id="test_connection_warehouse_pacflight")("warehouse_pacflight")

test_connection_dag()
