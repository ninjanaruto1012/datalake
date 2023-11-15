from datetime import datetime, timedelta
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow import DAG

dag_args = {
    'owner': 'phuong',
    'retry': 2,
    'retry_delay': timedelta(minutes=1)
}
with DAG(default_args=dag_args,
    dag_id='copy_cpustress_onvm209',
    start_date= datetime(2023, 9, 20)
    ) as dag:
    
    copy_sh_file = SSHOperator(ssh_conn_id='vm_acumos',
        task_id='copy_cpustress_sh',
        command = """
        sshpass -p "labadmin" scp /home/ubuntu/mon-script/cpustress.sh bpadmin@10.133.100.209:/home/bpadmin/phuong/mon-scripts
        """
        )
    copy_sh_file