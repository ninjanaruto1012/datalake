from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

ssh_hook = SSHHook(ssh_conn_id="vm_acumos", cmd_timeout=None)

default_arg={
    'owner': 'phuong',
    'retry': 2,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    default_args = default_arg,
    dag_id='cpu_collection_on_acumos_vm',
    start_date= datetime(2023, 9, 25, 19, 45, 00),
    schedule_interval='*/5 * * * *'
    ) as dag:
    generate_file = SSHOperator(ssh_hook=ssh_hook,
        task_id='run_python_to_collect_push_s3',
        command = """
        cd /home/ubuntu/mon-script/push-data-2-minio
        source minio-venv/bin/activate
        python push_data.py
        """
        )
    generate_file
    
    