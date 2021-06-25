import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import HTTPError
from getpass import getpass
import json


headers = {
    'accept': 'application/json',
    'Content-Type': 'application/json',
}

auth = HTTPBasicAuth('[username]', '[password]')        # input username and password.
dag_id = "[dag_id]"                                     # input dag_id to generate GET or POST request API.

# input the parameters when request sending.
body = {
  "conf": {"JOB_NAME": f"{dag_id}"}
}


try:
    # # GET: Only list DAG run, it is not trigger a new DAG run
    # response = requests.get(
    #     f"http://localhost:8080/api/v1/dags/{dag_id}/dagRuns", 
    #     headers=headers, auth=auth, data=json.dumps(body)
    # )
    
    # POST: Trigger a new DAG run
    response = requests.post(
        f"http://localhost:8080/api/v1/dags/{dag_id}/dagRuns",          # change ip or domain name in URL
        headers=headers, auth=auth, data=json.dumps(body)
    )

    response.encoding = 'utf-8'
    text = response.text
    print("text >>>> ", text)

    # If the response was successful, no Exception will be raised
    raise_for_status = response.raise_for_status()          # If it show None means no any Exception
    status_code = response.status_code                      # If status_code is 200 indicates that success, status_code is 404 indicates that Not Found.
    print("raise_for_status >>>> ", raise_for_status)
    print("status_code >>>> ", status_code)
    if status_code == 200:
        print('status_code indicates that Success!')
    elif status_code == 404:
        print('status_code indicates that Not Found.')

except HTTPError as http_err:
    print(f'HTTP error occurred: {http_err}')       # Python 3.6
except Exception as err:
    print(f'Other error occurred: {err}')           # Python 3.6
else:
    print('Success! Not HTTP error!')
