# from airflow.decorators import dag, task
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
# from tests.system.providers.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder
# sys_test_context_task = SystemTestContextBuilder().build()

default_arg={
    'owner': 'phuong',
    'retry': 2,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    default_args= default_arg,
    dag_id='dag2',
    start_date= datetime(2023, 9, 19)
) as dag:
    mdso_list = S3KeySensor(
            task_id='sensor_mdso_vm209log',
            bucket_name='mdso-vm-209',
            bucket_key='mon-vm.log',
            aws_conn_id='minio_s3'
            )
    mdso_list


