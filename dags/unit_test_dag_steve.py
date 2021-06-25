import pendulum
import airflow
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
import sys
sys.path.insert(0, "/opt/airflow/py_scripts")
sys.path.insert(0, "/opt/airflow/utils")

from utils_by_steve import to_bash_command
from utils_by_steve import func
from utils_by_steve import notify_email
import boto3

## System date
tw = pendulum.timezone('Asia/Taipei')
today = datetime.now(tw)

args = {"owner": "Steve",
	"start_date": datetime(2021, 3, 30),
	# 'on_failure_callback': notify_email,
    'retries': 0,
    'retry_delay': timedelta(seconds=10)
}

with DAG(dag_id='job_unit_test_steve', default_args=args, schedule_interval=None) as dag:
    func_pythonoperator = PythonOperator(task_id='unit_test_func_opt', python_callable=func, provide_context=True)
