from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow import DAG
from airflow.providers.ssh.hooks.ssh import SSHHook

ssh_hook = SSHHook(ssh_conn_id="vm_mdso207_conn", cmd_timeout=None)
dag_args = {
    'owner': 'phuong',
    'retry': 2,
    'retry_delay': timedelta(minutes=1)
}

with DAG(default_args=dag_args,
    dag_id='run_cpustress_onvm207',
    start_date= datetime(2023, 9, 20),
    schedule_interval = '0 13 * * *' 
    ) as dag:

    run_cpustress = SSHOperator(ssh_hook=ssh_hook,
        task_id='run_cpustress',
        command = """
        cd /home/bpadmin/phuong/mon-scripts
        chmod 777 cpustress.sh
        ./cpustress.sh
        """
        )
    
    
    run_cpustress
        
        