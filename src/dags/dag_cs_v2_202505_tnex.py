from airflow import DAG
from airflow.models import BaseOperator, Variable
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models.param import Param
from airflow.models.baseoperator import chain
from datetime import datetime, timedelta
import subprocess
import logging
import time
import json
import pytz
import requests

logger = logging.getLogger(__name__)

TIMEZONE = pytz.timezone("Asia/Ho_Chi_Minh")
DAG_NAME = "BLI_DAG_cs_v2_202505_tnex"
SCHEDULE = '30 7 * * 1'
DAG_ARGS = {    
    "start_date" : datetime(2024, 12, 1),
    "retries" : 3, 
    "retry_delay" : timedelta(minutes=10)
}
SPARK_INSTANCE_ID = Variable.get("BLI_SPARK_INSTANCE_ID")
USER = Variable.get("USER")
PASSWORD = Variable.get("PASSWORD")

volume_path = Variable.get("BLI_volume_path")
instance_path = Variable.get("BLI_instance_path")

# last_log = '' 
log_check_interval = 30

def spark_conf(config_type="small"):
    if config_type == 'tiny': 
        return {            
            "driver-memory": "1G",
            "driver-cores": 1,
            "executor-memory": "1G",
            "executor-cores": 1,
            "num-executors": 1
        }
    elif config_type == "small":
        return {            
            "driver-memory": "40G",
            "driver-cores": 5,
            "executor-memory": "40G",
            "executor-cores": 5,
            "num-executors": 11
        }
    
    elif config_type == "medium":
        return {            
            "driver-memory": "40G",
            "driver-cores": 5,
            "executor-memory": "40G",
            "executor-cores": 5,
            "num-executors": 19
        }
    
    else:
        return {            
            "driver-memory": "40G",
            "driver-cores": 5,
            "executor-memory": "40G",
            "executor-cores": 5,
            "num-executors": 25
        }


def send_msg_to_telegram(msg):
    bot_id = "7875188738:AAHGkJt-eXpQnt3DvNJlwQeHGq4N_xBQDrc"
    url = f"https://api.telegram.org/bot{bot_id}/sendMessage"
    params = {"chat_id": -1002184124681, "text": msg}
    proxy_server = {"http": "http://10.144.13.144:3129", "https": "http://10.144.13.144:3129"}
   
    try: 
        response = requests.post(url, params=params, proxies=proxy_server)
        result = response.json()

        if result['ok'] is not True:
            logger.warning("Tele msg failed")

    except Exception as e:
        logger.warning(f"FAILED send msg {e}")


def get_token(username, password):
    url = 'https://cpd-cp4dmedia.apps.cp4d.datalake.vnpt.vn/v1/preauth/validateAuth'
    header_1 = f'password: {password}'
    header_2 = f'username: {username}'
    result = subprocess.run(['curl','--location', '--request', 'GET', url ,'--header', header_1, '--header', header_2, '--data-raw',  '', '-k' ], capture_output=True)
    
    return json.loads(result.stdout.decode())['accessToken']


def call_api_run_code(access_token, spark_instance_id, python_file, arguments, config_type="small"):
    arguments += f" --dag_name {DAG_NAME}"
    arguments += f" --python_file {python_file.replace('.py', '').replace('/', '__')}"

    spark_resource = spark_conf(config_type)
    url = f'https://cpd-cp4dmedia.apps.cp4d.datalake.vnpt.vn/v2/spark/v3/instances/{spark_instance_id}/spark/applications'
    header_1 = f'Authorization: Bearer {access_token}'
    header_2 = 'Content-Type: application/json'
    data_raw = {
        "template_id": "spark-3.3-jaas-v2-cp4d-template",
        "application_details": {
            "application": f"/code/{python_file}",
            "conf": {
                "spark.datasource.singlestore.password": f"{PASSWORD}",
                "spark.datasource.singlestore.user": f"{USER}",
            },
            **spark_resource,
            "jars": "/code/libs/singlestore-spark-connector_2.12-4.1.3-spark-3.3.0.jar,/code/libs/mariadb-java-client-3.1.4.jar,/code/libs/singlestore-jdbc-client-1.1.5.jar,/code/libs/commons-dbcp2-2.9.0.jar,/code/libs/commons-pool2-2.11.1.jar,/code/libs/spray-json_3-1.3.6.jar",
            "arguments": [arguments],
            "env": {
                "RUNTIME_PYTHON_ENV": "python39",
                "PYTHONPATH": "/code/bli_release:/opt/ibm/spark/python/lib/pyspark.zip:/opt/ibm/spark/python/lib/py4j-0.10.9.5-src.zip:/opt/ibm/spark/jars/spark-core_2.12-3.3.1.jar:/home/spark/space/assets/data_asset:/home/spark/user_home/python-3:/cc-home/_global_/python-3:/home/spark/shared/user-libs/python:/home/spark/shared/conda/envs/python/lib/python/site-packages:/opt/ibm/conda/miniconda/lib/python/site-packages:/opt/ibm/third-party/libs/python3:/opt/ibm/image-libs/python3:/opt/ibm/image-libs/spark2/xskipper-core.jar:/opt/ibm/image-libs/spark2/spark-extensions.jar:/opt/ibm/image-libs/spark2/metaindexmanager.jar:/opt/ibm/image-libs/spark2/stmetaindexplugin.jar:/opt/ibm/spark/python:/opt/ibm/spark/python/lib/py4j-0.10.7-src.zip"
            },
        },
        "volumes": [
            {
            "name": f"cp4dmedia::{volume_path}",
                "mount_path": "/code"
            }
        ]
    }
    result = subprocess.run(['curl','--location', '--request', 'POST', url ,'--header', header_1, '--header', header_2, '--data-raw',  json.dumps(data_raw), '-k' ], capture_output=True)
    logger.info("json: " + subprocess.list2cmdline(['curl','--location', '--request', 'POST', url ,'--header', header_1, '--header', header_2, '--data-raw',  json.dumps(data_raw), '-k' ]))
    time.sleep(5)

    try:
        print(result.stdout.decode())
        return json.loads(result.stdout.decode())['application_id']
    except Exception as e:
        logger.info(result)
        msg = f"failed submit spark | dag {DAG_NAME} | node: {python_file}"
        send_msg_to_telegram(msg)
        raise Exception(msg, "error", e)


def get_job_status(access_token, spark_instance_id, job_id):
    # Get Spark job status
    url = f'https://cpd-cp4dmedia.apps.cp4d.datalake.vnpt.vn/v4/analytics_engines/{spark_instance_id}/spark_applications/{job_id}'
    header = f'Authorization: Bearer {access_token}'
    result = subprocess.run(['curl','--location', '--request', 'GET', url ,'--header', header, '-k'], capture_output=True)
    return json.loads(result.stdout.decode())['state']

def fetch_log(job_id):
    try: 
        request_str = f'https://cpd-cp4dmedia.apps.cp4d.datalake.vnpt.vn/zen-volumes/cp4dmedia::{instance_path}/v1/volumes/files/{SPARK_INSTANCE_ID}%2F{job_id}%2Flogs%2Fspark-driver-{job_id}-stdout'
        bearer = get_token(USER, PASSWORD)
        header_str = f'Authorization: Bearer {bearer}'
        print(request_str)
        curl_command = [
            "curl", "--location", "--request", "GET", request_str,
            "--header", header_str,
            "--header", "Content-Type: application/json",
            "--data-raw", "''",
            "-k"
        ]
        
        result = subprocess.run(curl_command, capture_output=True, text=True, check=True)
        return result.stdout
    except subprocess.CalledProcessError as e: 
        logger.error(f"Error: {e.returncode}")
        logger.error(f"STDOUT: {e.stdout}")
        logger.error(f"STDERR: {e.stderr}")
    except Exception as e: 
        logger.error(f"Error fetching log: {e}")

def check_log_progess(job_id, last_log, interval, python_file): 
    current_log = fetch_log(job_id)
    if current_log == last_log:
        msg = f"KILLED: '{DAG_NAME}' | node '{python_file}' | due to: no log update for {interval} minutes"
        kill_spark_job(job_id)
        send_msg_to_telegram(msg)
        raise Exception("Job log is not progressing!!!")
    return current_log

def kill_spark_job(job_id):
    request_str = f'https://cpd-cp4dmedia.apps.cp4d.datalake.vnpt.vn/v4/analytics_engines/{SPARK_INSTANCE_ID}/spark_applications/{job_id}'
    bearer = get_token(USER, PASSWORD)
    header_str = f'Authorization: Bearer {bearer}'
    
    curl_command = [
        "curl", "--location", "--request", "DELETE", request_str,
        "--header", header_str,
        "--data-raw", "''",
        "-k"
    ]
    
    subprocess.run(curl_command, capture_output=True, text=True, check=True)

def check_job_status(access_token, spark_instance_id, job_id, python_file):
    # Check Spark job status
    count = 0
    is_running = True
    
    last_log = fetch_log(job_id)
    while is_running:
        logger.info('Checking status ...')
        job_stt = get_job_status(access_token, spark_instance_id, job_id)
        if job_stt == "RUNNING":
            time.sleep(60)
            count += 1

            # fetch log every N mins 
            if count % log_check_interval == 0: 
                last_log = check_log_progess(job_id, last_log, log_check_interval, python_file)

        elif job_stt == "FINISHED":
            is_running = False

        elif job_stt == "FAILED":
            msg = f"FAILED: '{DAG_NAME}' | node '{python_file}'"
            send_msg_to_telegram(msg)

            msg = f"check job_id: {job_id}"
            send_msg_to_telegram(msg)

            raise Exception(msg)
        
        if count % 60 == 0:
            access_token = get_token(USER, PASSWORD)
            logger.info(f"access_token: {access_token}")


def run_flow(**kwargs):
    # Run flow function
    access_token = get_token(USER, PASSWORD)
    logger.info(f"access_token: {access_token}")

    # get args:
    spark_instance_id = kwargs['spark_instance_id']
    python_file = kwargs['python_file']
    arguments = kwargs['args']
    config_type = kwargs['config_type']

    # upt 
    update_id = call_api_run_code(access_token, spark_instance_id, "bli_release/src/tools/auto_upd.py", arguments, 'tiny')
    logger.info("upt")

    # send request
    job_id = call_api_run_code(access_token, spark_instance_id, python_file, arguments, config_type)
    logger.info(f"Job_id: {job_id}")
    check_job_status(access_token, spark_instance_id, job_id, python_file)


def create_operator(
    task_name:str, 
    py_file:str,
    config_type: str = "small",
    exe_args: dict = {},
    retries:int = 5
):
    kwargs = {}
    kwargs['python_file'] = py_file
    kwargs['config_type'] = config_type
    kwargs['args'] = exe_args
    kwargs['spark_instance_id'] = SPARK_INSTANCE_ID
    
    return PythonOperator(
        task_id = task_name,
        python_callable = run_flow, 
        retries=retries,
        provide_context=True,
        op_kwargs=kwargs
    )


### Params
dt_to = (datetime.now(TIMEZONE) - timedelta(days = 1)).strftime("%Y%m%d")
dt_from = (datetime.now(TIMEZONE) - timedelta(days = 2)).strftime("%Y%m%d")
run_mode = 'prod'
params = {
            'dt_from': Param(dt_from),
            'dt_to': Param(dt_to),
            'run_mode': Param(run_mode, enum = ['prod', 'backtest', 'dev']),
            'skip_dags': Param('')
        }

# Run DAG
with DAG(
        DAG_NAME,
        default_args = DAG_ARGS,
        catchup = False,
        render_template_as_native_obj = True,
        schedule_interval = None,
        max_active_runs = 1,
        params = params,
    ) as dag:

    input_arg = "--dt_from {{params.dt_from}} --dt_to {{params.dt_to}} --run_mode {{params.run_mode}} --skip_dags {{params.skip_dags}}"

    start_node = DummyOperator(
         task_id = 'start',
    )

    cs_v2_202505_tnex = create_operator(
        task_name = "cs_v2_202505_tnex",
        py_file = "bli_release/src/models/cs_v2_202505_tnex.py",
        config_type="huge",
        exe_args = input_arg,
    )
    
    end_node = DummyOperator(
        task_id = 'end',
    )


    start_node >> cs_v2_202505_tnex >> end_node