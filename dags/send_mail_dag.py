import pendulum
import airflow
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
import sys
sys.path.insert(0, "/opt/airflow/py_scripts")
sys.path.insert(0, "/opt/airflow/utils")

from utils_by_steve import to_bash_command
from utils_by_steve import func
from utils_by_steve import notify_email

## System date
tw = pendulum.timezone('Asia/Taipei')
today = datetime.now(tw)
today_date = today.strftime("%Y-%m-%d")


args = {"owner": "Steve",
	"start_date": days_ago(1),
    # 'email_on_retry': False,                    # bool, Indicates whether email alerts should be sent when a task is retried
    # 'email_on_failure': True,                   # bool, Indicates whether email alerts should be sent when a task failed
    # 'on_success_callback': notify_email,        # callable, much like the on_failure_callback except that it is executed when the task succeeds.
    'on_failure_callback': notify_email,        # callable, a function to be called when a task instance of this task fails.
    'retries': 0,                               # int, the number of retries that should be performed before failing the task
    'retry_delay': timedelta(seconds=10)        # minutes=5
}

with DAG(dag_id="send_mail", default_args=args, description='airflow send mailing if the DAG status failed.', 
            schedule_interval=None) as dag:


    send_mail_operator = PythonOperator(
            task_id='send_mail',
            provide_context=True,
            python_callable=func,
            dag=dag)


send_mail_operator