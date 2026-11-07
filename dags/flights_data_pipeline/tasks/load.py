from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException, AirflowException
from helper.minio import CustomMinio
from pangres import upsert
from sqlalchemy import create_engine
from flights_data_pipeline.tasks.window import object_name as build_object_name
import logging


class Load:
    @staticmethod
    def _pacflight_db(table_name, incremental, **kwargs):
        """
        Load data from CSV in MinIO to staging PostgreSQL DB.
        """
        logging.info(f"[Load] Starting load for table: {table_name}")
        date = kwargs.get("ds")
        ti = kwargs["ti"]

        # Cek hasil extract
        extract_result = ti.xcom_pull(task_ids=f"extract.{table_name}")
        logging.info(f"[Load] Extract result for {table_name}: {extract_result}")

        if not extract_result or extract_result.get("status") != "success":
            logging.info(f"[Load] Skipping {table_name} due to extract status: {extract_result}")
            raise AirflowSkipException(f"[Load] Skipped {table_name} karena tidak ada data dari extract.")

        table_pkey = kwargs.get("table_pkey")
        object_name = build_object_name(table_name, incremental, kwargs["data_interval_start"])
        bucket_name = "extracted-data"

        engine = create_engine(PostgresHook(postgres_conn_id="warehouse_pacflight").get_uri())

        try:
            logging.info(f"[Load] Downloading {object_name} from bucket {bucket_name}")
            df = CustomMinio._get_dataframe(bucket_name, object_name)

            if df.empty:
                logging.warning(f"[Load] Dataframe is empty for {table_name}. Skipping.")
                ti.xcom_push(key="return_value", value={"status": "skipped", "data_date": date})                
                raise AirflowSkipException(f"[Load] Skipping {table_name}: CSV is empty")

            df = df.set_index(table_pkey[table_name])

            upsert(
                con=engine,
                df=df,
                table_name=table_name,
                schema="stg",
                if_row_exists="update"
            )

            logging.info(f"[Load] Load success for {table_name}, {len(df)} records inserted/updated.")
            ti.xcom_push(key="return_value", value={"status": "success", "data_date": date})            

        except AirflowSkipException as e:
            logging.warning(str(e))
            raise e

        except Exception as e:
            logging.error(f"[Load] Failed to load {table_name}: {str(e)}")
            raise AirflowException(f"[Load] Failed to load {table_name}: {str(e)}")

        finally:
            engine.dispose()
