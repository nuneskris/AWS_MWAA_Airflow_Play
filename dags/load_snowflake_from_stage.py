from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Fetch Airflow Variables with error handling
try:
    destination_bucket = Variable.get("destination_bucket")
    src_prefix = Variable.get("src_prefix")
    dest_prefix = Variable.get("dest_prefix")
except KeyError as e:
    logger.error(f"Variable {str(e)} not found in Airflow Variables. Please add it.")
    raise

now = datetime.now()

DEFAULT_ARGS = {
    'owner': 'airflow',
    'snowflake_conn_id': 'snowflake_conn_accountadmin',
    'depends_on_past': False,
    'start_date': datetime(now.year, now.month, now.day, now.hour),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

def move_files_s3(src_bucket, src_prefix, dest_bucket, dest_prefix, aws_conn_id='aws_default'):
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    
    try:
        # List files in source prefix
        files = s3.list_keys(bucket_name=src_bucket, prefix=src_prefix)
        if not files:
            logger.info(f'No files found in {src_bucket}/{src_prefix}')
            return
        
        for file_key in files:
            src_key = file_key
            dest_key = file_key.replace(src_prefix, dest_prefix, 1)

            logger.info(f'Moving file from {src_bucket}/{src_key} to {dest_bucket}/{dest_key}')
            
            # Copy the file to the destination
            s3.copy_object(
                source_bucket_name=src_bucket,
                source_bucket_key=src_key,
                dest_bucket_name=dest_bucket,
                dest_bucket_key=dest_key
            )

            # Delete the file from the source
            s3.delete_objects(bucket=src_bucket, keys=src_key)
            logger.info(f'Deleted file from {src_bucket}/{src_key}')
    except Exception as e:
        logger.error(f'Error while moving files: {str(e)}')
        raise

with DAG(
    dag_id=os.path.basename(__file__).replace(".py", ""),
    default_args=DEFAULT_ARGS,
    tags=['Snowflake', 'kfnstudy', 'DAG_LoadDataS3SnowflakeMoveAfter'],
    schedule_interval=None
) as dag:

    # Create database
    loaddatabase = SnowflakeOperator(
        task_id='load_database',
        sql='kfnstudy_snowflake_queries/load_database.sql'
    )

    move_files_task = PythonOperator(
        task_id='move_files_s3_task',
        python_callable=move_files_s3,
        op_kwargs={
            'src_bucket': destination_bucket,  # Source bucket name
            'src_prefix': src_prefix,  # Source folder path
            'dest_bucket': destination_bucket,  # Destination bucket name
            'dest_prefix': dest_prefix,  # Destination folder path
        },
    )

    loaddatabase >> move_files_task
