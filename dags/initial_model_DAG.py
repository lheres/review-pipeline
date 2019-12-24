import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from extract import staging_reviews

REVIEW_STREAM_SAMPLE = "/data/item_sample.json.gz"
METADATA_STREAM_SAMPLE = "/data/metadata_sample.json.gz"
TARGET_SCHEMA = 'dbt_reviews'
TARGET_TABLE_REVIEWS = 'raw_reviews'
TARGET_TABLE_METADATA = 'raw_metadata'

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),      # this in combination with catchup=False ensures the DAG being triggered from the current date onwards along the set interval
    'provide_context': True,                            # this is set to True as we want to pass variables on from one task to another
}

dag = DAG(
    dag_id='initial_model_DAG',
    default_args=args,
    schedule_interval= '@once',             # set interval
	catchup=False,                          # indicate whether or not Airflow should do any runs for intervals between the start_date and the current date that haven't been run thus far
)


task1 = PythonOperator(
    task_id='load_staging',
    python_callable=staging_reviews.load,        # function to be executed
    op_kwargs={'path': REVIEW_STREAM_SAMPLE,        # input arguments
			   'target_schema': TARGET_SCHEMA,
               'target_table': TARGET_TABLE_REVIEWS},
    dag=dag,
)

task2 = PythonOperator(
    task_id='load_staging',
    python_callable=staging_reviews.load,        # function to be executed
    op_kwargs={'path': METADATA_STREAM_SAMPLE,        # input arguments
			   'target_schema': TARGET_SCHEMA,
               'target_table': TARGET_TABLE_METADATA},
    dag=dag,
)


task1 >> task2                  # set task priority
