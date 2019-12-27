from datetime import timedelta

import airflow
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator

from extract.dbtRPCClientRunner import DBTRPCClient
from extract import staging_reviews

REVIEW_STREAM_SAMPLE = Variable.get("review_location"
                                    , default_var="/usr/local/airflow/data/item_dedup.json.gz")
METADATA_STREAM_SAMPLE = Variable.get("metadata_location"
                                    , default_var="/usr/local/airflow/data/metadata.json.gz")
TARGET_SCHEMA = 'dbt_reviews'
TARGET_TABLE_REVIEWS = 'raw_reviews'
TARGET_TABLE_METADATA = 'raw_metadata'

def run_dbt(task=None, models=None, exclude=None, **kwargs):

    dbt_client = DBTRPCClient()
    dbt_client.execute(task=task, models=models, exclude=exclude)

    return None

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),      # this in combination with catchup=False ensures the DAG being triggered from the current date onwards along the set interval
    'provide_context': True,                            # this is set to True as we want to pass variables on from one task to another
}

dag = DAG(
    dag_id='review_pipeline_DAG',
    default_args=args,
    schedule_interval=timedelta(hours=8),             # set interval
	catchup=False,                                    # indicate whether or not Airflow should do any runs
                                                      # for intervals between the start_date and the current date
                                                      # that haven't been run thus far
)

task0 = LatestOnlyOperator(
    task_id = 'latest',
    dag = dag,
)

task1 = PythonOperator(
    task_id='load_staging_reviews',
    python_callable=staging_reviews.load,        # function to be executed
    op_kwargs={'path': REVIEW_STREAM_SAMPLE,        # input arguments
			   'target_schema': TARGET_SCHEMA,
               'target_table': TARGET_TABLE_REVIEWS,
               'rem_fields': ['reviewText']},
    dag=dag,
)

task2 = PythonOperator(
    task_id='load_staging_metadata',
    python_callable=staging_reviews.load,        # function to be executed
    op_kwargs={'path': METADATA_STREAM_SAMPLE,        # input arguments
			   'target_schema': TARGET_SCHEMA,
               'target_table': TARGET_TABLE_METADATA,
               'rem_fields': ['description']},
    dag=dag,
)

task3 = PythonOperator(
    task_id='create_dbt_models',
    python_callable=run_dbt,
    dag=dag,
    op_kwargs={'task': 'run', 'models': '', 'exclude': ''},
)

task0 >> task1 >> task2 >> task3                 # set task priority
