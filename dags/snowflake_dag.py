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

## System date
tw = pendulum.timezone('Asia/Taipei')
today = datetime.now(tw)
today_date = today.strftime("%Y-%m-%d")

args = {"owner": "Steve",
	"start_date": days_ago(1),
    # 'email_on_failure': True,
    # 'on_failure_callback': notify_email,
    'retries': 0,
    'retry_delay': timedelta(seconds=10)
}

with DAG(dag_id="snowflake_dag", default_args=args, schedule_interval=None) as dag:
  
    #### adding op_kwargs to run PythonOperator
    '''
    case 2:
    Testing: the ingestion of three jobs, bring different parameters in "specific_data_source" to execution job.
    '''
    ## SNOW_FLAKE_CREATE_TABLES
    sf_create_tables_opt_pythonoperator = PythonOperator(
        task_id='create_tables',
        python_callable=func, provide_context=True
        )

    ## AAA_TO_S3_DATA_INGESTION
    gateway_aaa_to_s3_data_ingestion_opt_pythonoperator = PythonOperator(
        task_id='aaa_ingestion',
        python_callable=func, provide_context=True,
        op_kwargs={
                'src_path': 'ingestion',
                'specific_data_source': 'GATEWAY_AAA'
                }
        )

    ## BBB_TO_S3_DATA_INGESTION
    gateway_bbb_to_s3_data_ingestion_opt_pythonoperator = PythonOperator(
        task_id='bbb_ingestion',
        python_callable=func, provide_context=True,
        op_kwargs={
                'src_path': 'ingestion',
                'specific_data_source': 'GATEWAY_BBB'
                }
        )

    ## CCC_TO_S3_DATA_INGESTION
    gateway_ccc_to_s3_data_ingestion_opt_pythonoperator = PythonOperator(
        task_id='ccc_ingestion',
        python_callable=func, provide_context=True,
        op_kwargs={
                'src_path': 'ingestion',
                'specific_data_source': 'GATEWAY_CCC'
                }
        )

    ## SF-S3_PARQUET_TO_SNOWFLAKE
    sf_s3_parquet_to_snowflake_opt_pythonoperator = PythonOperator(
        task_id='s3_to_snowflake',
        python_callable=func, provide_context=True
        )    


    #### PythonOperator running
    '''
    case 1:
    if it input params on UI, it is success all done.
    Testing for Job name given correct to output tabls in each jobs.
    {   
        "JOB_NAME": "billing",
        "end_date": "2021-05-24",
        "start_date": "2021-05-24"
    }
    '''
    ## BILLING
    billing_opt_pythonoperator = PythonOperator(
        task_id='billing',
        python_callable=func, provide_context=True
        )   

    ## ACCOUNT_ISSURANCE
    account_issurance_opt_pythonoperator = PythonOperator(
        task_id='account_issurance',
        python_callable=func, provide_context=True
        )


sf_create_tables_opt_pythonoperator >> gateway_aaa_to_s3_data_ingestion_opt_pythonoperator >> sf_s3_parquet_to_snowflake_opt_pythonoperator
sf_create_tables_opt_pythonoperator >> gateway_bbb_to_s3_data_ingestion_opt_pythonoperator >> sf_s3_parquet_to_snowflake_opt_pythonoperator
sf_create_tables_opt_pythonoperator >> gateway_ccc_to_s3_data_ingestion_opt_pythonoperator >> sf_s3_parquet_to_snowflake_opt_pythonoperator
billing_opt_pythonoperator >> account_issurance_opt_pythonoperator

