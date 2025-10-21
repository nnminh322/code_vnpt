from airflow import DAG
from airflow.models.param import Param
from airflow.models import BaseOperator, Variable
from airflow.operators.python import PythonOperator
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
import subprocess
import logging
import time
import json
import pytz
import requests

# get logger
logger = logging.getLogger(__name__)

# Define global variables
TIMEZONE = pytz.timezone("Asia/Ho_Chi_Minh")
DAG_NAME = "blueinfo_flow_release"
DAG_ARGS = {
    "start_date": datetime(2024, 8, 8),
    "retries": 0, 
    "retry_delay": timedelta(minutes=10)
}
SCHEDULE  = '30 7 16 * *'

SPARK_INSTANCE_ID = Variable.get("BLI_SPARK_INSTANCE_ID")
USER = Variable.get("USER")
PASSWORD = Variable.get("PASSWORD")

volume_path = Variable.get("BLI_volume_path")
instance_path = Variable.get("BLI_instance_path")

# Function to create operator for running flow
def create_operator(
    task_name:str, 
    py_file:str,
    exe_args: dict = {},
    retries:int = 2
) -> BaseOperator:
    kwargs = {}
    kwargs['python_file'] = py_file
    kwargs['args'] = exe_args
    kwargs['spark_instance_id'] = SPARK_INSTANCE_ID
    return PythonOperator(
        task_id = task_name,
        python_callable = run_flow, 
        retries=retries,
        provide_context=True,
        op_kwargs=kwargs,
        
    )

# Function for sending messages to Telegram
def send_msg_to_telegram(msg):
    bot_id = "7795284458:AAGPJp_MRX9OGQu5adJWJNESEh2q0c8r0Cg"
    url = f"https://api.telegram.org/bot{bot_id}/sendMessage"
    params = {"chat_id": -1002184124681, "text": msg}
    proxy_server = {"http": "http://10.144.13.144:3129", "https": "http://10.144.13.144:3129"}
    
    response = requests.post(url, params=params, proxies=proxy_server)
    print(response.json())    

# Get token function
def get_token(username, password):
    url = 'https://cpd-cp4dmedia.apps.cp4d.datalake.vnpt.vn/v1/preauth/validateAuth'
    header_1 = f'password: {password}'
    header_2 = f'username: {username}'
    result = subprocess.run(['curl','--location', '--request', 'GET', url ,'--header', header_1, '--header', header_2, '--data-raw',  '', '-k' ], capture_output=True)
    return json.loads(result.stdout.decode())['accessToken']

# Function for submiting Spark job
def call_api_run_code(access_token, spark_instance_id, python_file, arguments):
    url = f'https://cpd-cp4dmedia.apps.cp4d.datalake.vnpt.vn/v2/spark/v3/instances/{spark_instance_id}/spark/applications'
    header_1 = f'Authorization: Bearer {access_token}'
    header_2 = 'Content-Type: application/json'
    arguments = arguments
    data_raw = {
        "template_id": "spark-3.3-jaas-v2-cp4d-template",
        "application_details": {
            "application": f"/code/bli_release/{python_file}",
            
            "conf": {
                "spark.datasource.singlestore.password": f"{PASSWORD}",
                "spark.datasource.singlestore.user": f"{USER}",
            },
            "driver-memory": "50G",
            "driver-cores": 5, 
            "executor-memory": "50G", 
            "executor-cores": 5, 
            "num-executors": 24,
            "jars": "/code/libs/singlestore-spark-connector_2.12-4.1.3-spark-3.3.0.jar,/code/libs/mariadb-java-client-3.1.4.jar,/code/libs/singlestore-jdbc-client-1.1.5.jar,/code/libs/commons-dbcp2-2.9.0.jar,/code/libs/commons-pool2-2.11.1.jar,/code/libs/spray-json_3-1.3.6.jar",
            "arguments": arguments,
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
    print("json: " + subprocess.list2cmdline(['curl','--location', '--request', 'POST', url ,'--header', header_1, '--header', header_2, '--data-raw',  json.dumps(data_raw), '-k' ]))
    return json.loads(result.stdout.decode())['application_id']

# Get Spark job status
def get_job_status(access_token, spark_instance_id, job_id):
    url = f'https://cpd-cp4dmedia.apps.cp4d.datalake.vnpt.vn/v4/analytics_engines/{spark_instance_id}/spark_applications/{job_id}'
    header = f'Authorization: Bearer {access_token}'
    result = subprocess.run(['curl','--location', '--request', 'GET', url ,'--header', header, '-k'], capture_output=True)
    return json.loads(result.stdout.decode())['state']

# Check Spark job status
def check_job_status(access_token, spark_instance_id, job_id, python_file, all_months, fix_date):
    count = 0
    is_running = True
    while is_running:
        print('Checking status ...')
        job_stt = get_job_status(access_token, spark_instance_id, job_id)
        if job_stt == "RUNNING":
            time.sleep(60)
            count += 1
        elif job_stt == "FINISHED":
            print('FINISHED')
            send_msg_to_telegram(f"{DAG_NAME}: Finished {str(python_file[:-2])} months: {all_months}, fix date: {fix_date}, run mode: {run_mode}")
            is_running = False
        elif job_stt == "FAILED":
            send_msg_to_telegram(f"{DAG_NAME}: Run failed {str(python_file[:-2])} months: {all_months}, fix date: {fix_date}, run mode: {run_mode}")
            raise Exception("RUN FAILED")
        if count % 60 == 0:
            access_token = get_token(USER, PASSWORD)
            logger.info(f"access_token: {access_token}")

# Run flow function
def run_flow(**kwargs):
    access_token = get_token(USER, PASSWORD)
    logger.info(f"access_token: {access_token}")

    # get args:
    spark_instance_id = kwargs['spark_instance_id']
    python_file = kwargs['python_file']
    arguments = kwargs['args']
    job_id = call_api_run_code(access_token, spark_instance_id, python_file, arguments)
    logger.info(f"Job_id: {job_id}")
    check_job_status(access_token, spark_instance_id, job_id, python_file, all_months, fix_date)

# List of files in flow
file_list = [
    'flow_vas_2friend.py',
    'flow_vascdr_udv.py',
    'flow_vascdr_utn.py',
    'flow_vascloud_da.py',
    'flow_prepaid_and_danhba.py',
    'flow_no.py',
    'flow_tra.py',
    'flow_cv207.py',
    'flow_3g_subs.py',
    'flow_gsma.py',
    'flow_air.py',
    'flow_vas_brandname.py',
    'flow_subscriber.py',
    'flow_voice_volte.py',
    'flow_voice_msc.py',
    'flow_usage.py',
    'flow_smrs_1.py',
    'flow_smrs_2.py',
    'flow_smrs_3.py',
    'flow_smrs_4.py',
    'flow_ggsn.py',
    'flow_device.py',
    'flow_merge_new.py',
    'flow_credit_score_predict.py',
    # 'flow_leadgen_score_predict.py'
]

# Define date and month for running flow
fix_days = [1, 15]
all_months = []
for i in range(0, 6):
    if datetime.today().day > fix_days[0] and datetime.today().day < fix_days[1]:
        month = (datetime.today() - relativedelta(months=i)).strftime('%Y%m')
        all_months.append(month)
        fix_date = ['01']
    else:
        month = (datetime.today() - relativedelta(months=i)).strftime('%Y%m')
        all_months.append(month)
        fix_date = ['15']
run_mode = 'prod'

# Run DAG
with DAG(
        DAG_NAME,
        default_args = DAG_ARGS,
        catchup = False,
        user_defined_macros = {},
        tags = [],
        render_template_as_native_obj = True,
        schedule_interval = SCHEDULE,
        max_active_runs = 1,
        params = {
            'all_months': Param(all_months, type = 'array'),
            'fix_date': Param(fix_date, type = 'array'),
            'run_mode': Param(run_mode, enum = ['prod', 'backtest'])
        }

    ) as dag:
    input_arg = '--all_months: {{params.all_months}}', '--fix_date: {{params.fix_date}}', '--run_mode: {{params.run_mode}}'
    task_list = []
    for f in file_list:
        file = 'src/code_v31/' + f
        complete_flow = create_operator(task_name=file.replace(".py", "").replace('/', '__'), py_file=file, exe_args=input_arg)
        task_list.append(complete_flow)

    # Set downstream relationships for task failure handling
    for i in range(len(task_list)-1):
        task_list[i].set_downstream(task_list[i+1])