from airflow import DAG

from datetime import datetime, timedelta

from airflow.contrib.operators.ssh_operator import SSHOperator
# from airflow.providers.ssh.hooks.ssh import SSHHook

default_arg={
    'owner': 'phuong',
    'retry': 2,
    'retry_delay': timedelta(minutes=1)
}
#  def get_ssh_connection():
# sshHook = SSHHook(ssh_conn_id="vm_acumos")
#     conn = sshHook.get_conn()
    
     
        
with DAG(
    default_args= default_arg,
    dag_id='check_ssh_acumos',
    start_date= datetime(2023, 9, 19)
) as dag:
    
    t1 = SSHOperator(ssh_conn_id='vm_acumos',
    task_id='ssh_operator',
    command = """
    cd phuong
    mkdir test-script
    cd test-script
    touch test.sh
    """
    )
    # command='cd mon-script\
    #         chmod 777 stats.sh\
    #         nohup ./stats.sh')
    
    t1