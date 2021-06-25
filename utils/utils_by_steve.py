import os
import sys
import ast
import json
import logging
import importlib
import subprocess
import traceback
from airflow.utils.state import State
from airflow.exceptions import AirflowException
base_path = "/opt/airflow/py_scripts"
sys.path.append(base_path)
sys.path.insert(0, "/opt/airflow/utils")
import aws
import sendemail as se


def to_bash_command(script_path: str, default_params: dict = {}):
    script_path_split = script_path.split("/")
    pyfile = script_path_split[-1] if script_path.endswith(".py") else "main.py"
    script_path = script_path_split[0]

    file_path = f"{base_path}/{script_path}"

    bash_command = f"{file_path}/{pyfile}"

    parameters = {}
    if "parameters.py" in os.listdir(file_path):
        param = importlib.import_module(f"{script_path}.parameters")
        parameters = ast.literal_eval(json.dumps(param.parameters))
    parameters.update(default_params)

    params_command = ""
    for k, v in parameters.items():
        params_command += f" --{k} '{v}'"
    print("bash_command >>>> ", bash_command)
    print("params_command >>>> ", params_command)
    return bash_command, params_command



def func(**context):
    print("context: >>>> ", context)
    conf = context['dag_run'].conf
    # Add job_run_id in each conf
    conf['JOB_RUN_ID'] = context['dag_run'].run_id

    # If it's unit test function, then get it from the web. If not, then JOB_NAME should be set in the conf.
    if "manual" in os.environ['AIRFLOW_CTX_DAG_RUN_ID'] and 'unit_test_func_opt' in os.environ['AIRFLOW_CTX_TASK_ID']:
        # job_name = conf['JOB_NAME']
        job_name = context['params'].get('JOB_NAME')
        src_path = job_name
        print("manual in unit_test, job_name: ", job_name)
        print("manual in unit_test, src_path: ", src_path)
    elif 'src_path' in context:
        # If src_path is not None, then src_path is not equals to job_name
        job_name = os.environ['AIRFLOW_CTX_TASK_ID']
        src_path = context['src_path']
        print("src_path in context, job_name: ", job_name)
        print("src_path in context, src_path: ", src_path)
    else:
        # Default: to use task_id to set the job_name
        job_name = os.environ['AIRFLOW_CTX_TASK_ID']
        src_path = job_name
        print("job_name: ", job_name)
        print("src_path: ", src_path)
    conf['JOB_NAME'] = job_name
    
    if 'specific_data_source' in context:
        conf['specific_data_source'] = context['specific_data_source']
        print("specific_data_source: ", conf['specific_data_source'])
    else:
        print("specific_data_source is not in context!")

    cmd, pmd = to_bash_command(src_path, conf)
    print("cmd >>>> ", cmd)
    print("pmd >>>> ", pmd)
    try:
        outs = subprocess.check_output(f"python {cmd} {pmd}; exit 0", shell=True, stderr=subprocess.STDOUT).decode("utf-8").split('\n')    # original 
        for i, out in enumerate(outs):
            if "Traceback" in out:
                print("\n\n" + "-" * 120)
                logging.error("\n".join(outs[i:]))
            print(out)
        print("outs >>>> try to find exception for real error >>>> ", outs)
        ## Whatever main.py with try catch or not, then catch "Traceback (most recent call last):" text in outs
        if "Traceback (most recent call last):" in outs:
            context['ti'].state = State.FAILED          # setup status for job failed, whatever main.py with try catch or not
        context['exception'] = outs
    except subprocess.CalledProcessError as e:
        print("\n\n" + "-" * 120)
        print(e.output)
        context['ti'].state = State.FAILED
        context['exception'] = e.output
    
    if context['ti'].state == State.FAILED:
        notify_email(context)


#### Temporary turn off send mail for testing
def notify_email(kwargs):
    """Setting SMTP from AWS."""
    SMTP_mail_secret_name = ""                     # setting up your AWS secret name
    email_creds = aws.get_secret(SMTP_mail_secret_name, '[regoin]')        # setting the regoin to credentials
    emailfrom = email_creds['accountname']
    emailsto = ['[mail receiver]']                  # setting up mail receiver
    emailscc = ['[mail cc ]']                      # setting up mail cc
    print(f"Sender: {emailfrom}")

    username = email_creds['username']
    password = email_creds['password']
    server = email_creds['server']
    print(f"Server: {server}")

    """Send custom email alerts."""
    print("kwargs >>>> ", kwargs)
    ti = kwargs['ti']
    dag_run = kwargs['dag_run']
    var = kwargs['var']['json']
    params = kwargs['params']
    print(f"ti: {ti}")
    print(f"dag_run: {dag_run}")

    ### Get exception then parsing it
    if kwargs.get('exception') is not None and type(kwargs.get('exception')) == list:
        dh_excpt = "During handling of the above exception, another exception occurred:"
        matching_main = [s for s in kwargs['exception'] if "/main.py" in s]
        print("matching_main >>>> ", matching_main)
    
        if matching_main != []:
            matching_fist_text = matching_main[0]
            print("matching_fist_text >>>> ", matching_fist_text)
            matching_fist_index = kwargs['exception'].index(matching_fist_text)
            print("matching_fist_index >>>> ", matching_fist_index)

            matching_last_text = matching_main[-1]
            print("matching_last_text >>>> ", matching_last_text)
            matching_last_index = kwargs['exception'].index(matching_last_text)
            print("matching_last_index >>>> ", matching_last_index)

            if dh_excpt in kwargs['exception']:
                dhe_index = kwargs['exception'].index(dh_excpt)
                print("The index of dhe >>>> ", dhe_index)

                if matching_fist_index < dhe_index:
                    # when "/main.py" first show before "During handling..." then remove after "During handling..." text until the end
                    kwargs['exception'][dhe_index:] = []
                elif matching_fist_index > dhe_index:
                    # when "/main.py" first show after "During handling..." then remove after another text until the end
                    kwargs['exception'][matching_last_index+2:] = []

        formatted_exception = "\n".join(kwargs['exception'])
        print(f"formatted_exception: {formatted_exception}")
    elif kwargs.get('exception') is not None: 
        formatted_exception = kwargs['exception']
        print(f"formatted_exception: {formatted_exception}")

    title = ''
    body = ''
    print("dag_run.run_id >>>> ", dag_run.run_id)
    print("ti.task_id >>>> ", ti.task_id)
    print("ti.state >>>> ", ti.state)

    print("When ti.state == State.FAILED >>>> ")                    # ti.state == State.FAILED as same as ti.state == 'failed'
    title = f"[TEST] Airflow alert: ({dag_run.run_id}) failed on ({ti.task_id})"
    body = f"Dears, \n\n\n" + \
        f"The job_id ({dag_run.run_id}) failed on ({ti.task_id}). \n" + \
        f"Check what goes wrong, the ERROR message is shown as below: \n\n" + \
        f"{formatted_exception} \n\n" + \
        f"Forever yours, \n" + \
        f"RDP Data Team"
    print("check title >>>> \n", title)
    print("check body  >>>> \n", body)
    print(f"Prepare to send out the mail...\n\t\tsubject: {title}")  
    se.email(emailfrom, emailsto, emailscc, username, password, server, body, subject = title)
    print("The email send out done.")
    raise AirflowException(f"AirflowException: Pleaes check what goes wrong this job_id ({dag_run.run_id}) failed on ({ti.task_id}).")
