# Airflow-extended-functions
Developed end-to-end extended functions for Airflow by Steve

# Reference Apache Airflow Documents
## Setting up Running Airflow in Docker
http://apache-airflow-docs.s3-website.eu-central-1.amazonaws.com/docs/apache-airflow/latest/start/docker.html

## utils_by_steve.py; Author: Steve.
Prepare in advance.
1. Setting up AWS credential.
2. Each job generate as same as job name in S3 path, it contains main.py and parameter.py in folder.
3. Upload dags, utils and py_script from S3 to EC2. 
(in EC2 within docker path such as: /opt/airflow/py_scripts)
When trigger DAG running
1. Airflow task will callable func function from utils_by_steve.py
2. It will take up func to setup parameters. If there are op_kwargs in task, it will take up func with context together.
3. Then it callable to_bash_command after func setted up.
4. to_bash_command function parse the parameters from parameter.py and transform bash parameters string return back.
5. subprocess.check_output execute the main.py and parameters string by bash scripting.
6. It will catch traceback information, if it happened exception.
7. It set up state failed and exception in context then it callable notify_email function to send the error message mailing.


## Airflow Stable REST API
http://apache-airflow-docs.s3-website.eu-central-1.amazonaws.com/docs/apache-airflow/latest/stable-rest-api-ref.html
## airflow_trigger_api.py; Author: Steve.
Prepare in advance.
1. Setting up the API authorization (backend.basic_auth or backend.defult)
2. Use airflow_trigger_api.py to send the API request.
3. Use swagger to test airflow API as well.  http://localhost:8080/api/v1/ui/#/DAGRun/get_dag_runs
4. Notaion: GET will list DAG historical run and POST will triigger a new DAG run.

### Contact me if you have any quetions about my code
e-mail: spark82475@gmail.com