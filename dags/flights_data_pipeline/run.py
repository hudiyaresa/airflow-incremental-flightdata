from airflow.decorators import dag, task_group
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from helper.callbacks.slack_notifier import slack_notifier
from pendulum import datetime

from flights_data_pipeline.tasks.extract import Extract
from flights_data_pipeline.tasks.load import Load
from flights_data_pipeline.tasks.transform import QUERY_DIR, Transform

# Fallbacks keep the DAG parseable before include/variables.json is imported
DEFAULT_TABLES = [
    "aircrafts_data", "airports_data", "bookings", "tickets",
    "seats", "flights", "ticket_flights", "boarding_passes",
]
DEFAULT_TABLE_PKEYS = {
    "aircrafts_data": "aircraft_code",
    "airports_data": "airport_code",
    "bookings": "book_ref",
    "tickets": "ticket_no",
    "seats": ["aircraft_code", "seat_no"],
    "flights": "flight_id",
    "ticket_flights": ["ticket_no", "flight_id"],
    "boarding_passes": ["ticket_no", "flight_id"],
}


def is_incremental():
    return str(Variable.get("incremental", default_var="True")).lower() == "true"


default_args = {
    'on_failure_callback': slack_notifier
}

# ========== Task Groups ==========

@task_group(group_id="extract")
def extract_group(table_list):
    incremental = is_incremental()
    for table in table_list:
        PythonOperator(
            task_id=f"{table}",
            python_callable=Extract._pacflight_db,
            op_kwargs={'table_name': table, 'incremental': incremental},
            do_xcom_push=True
        )        

@task_group(group_id="load")
def load_group(tables_with_pkey):
    incremental = is_incremental()
    load_tasks = []

    for table, pkey in tables_with_pkey.items():
        task = PythonOperator(
            task_id=f"{table}",
            python_callable=Load._pacflight_db,
            op_kwargs={
                'table_name': table,
                'incremental': incremental,
                'table_pkey': tables_with_pkey
            },
            trigger_rule=TriggerRule.NONE_FAILED                        
        )
        load_tasks.append(task)

    # Set sequential load due to FK dependencies
    for i in range(1, len(load_tasks)):
        load_tasks[i - 1] >> load_tasks[i]

@task_group(group_id="transform")
def transform_group(transform_tables):
    previous = None
    for table in transform_tables:
        transform = Transform.build_operator(
            task_id=f"{table}",
            table_name=table,            
            sql_dir="final"
        )

        transform.trigger_rule = TriggerRule.NONE_FAILED

        if previous:
            previous >> transform
        previous = transform


# ========== Main DAG ==========

@dag(
    dag_id='flights_data_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    template_searchpath=[str(QUERY_DIR)],
    tags=['pacflight', 'ETL']
)
def flights_data_pipeline():

    tables_to_extract = Variable.get("tables_to_extract", default_var=DEFAULT_TABLES, deserialize_json=True)
    tables_to_load = Variable.get("tables_to_load", default_var=DEFAULT_TABLE_PKEYS, deserialize_json=True)

    transform_tables = [
        'dim_aircraft', 'dim_airport', 'dim_seat', 'dim_passenger',
        'fct_boarding_pass', 'fct_booking_ticket',
        'fct_seat_occupied_daily', 'fct_flight_activity'
    ]

    # Run task groups
    extract = extract_group(tables_to_extract)
    load = load_group(tables_to_load)
    transform = transform_group(transform_tables)

    # Dependency flow
    extract >> load >> transform

flights_data_pipeline()