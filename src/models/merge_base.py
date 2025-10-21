import sys
from src.utils import common as utils 
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta, MO
from pyspark.sql import functions as F

def load_and_merge_dataset(spark, feature_dict, common_config, run_date_str:str, run_mode:str):
    df = None
    feature_destination = common_config['feature_destination']

    for table, dct in feature_dict.items(): 
        lst_features = dct['num'] + dct['cat']
        print(f"Loading: {table} table")
        
        if table == 'usage': 
            sub_df = None
            directory = 'prod/features/usage/category/final_fts/'
            for cat in ['total', 'data', 'sms', 'video', 'voice', 'voip']: 
                sub_df_1 = utils.load_from_s3(spark, directory.replace("category", cat)).where(f"date = {run_date_str}")
                sub_df = sub_df_1 if sub_df is None else sub_df.join(sub_df_1, "msisdn", 'outer')

        elif table == 'smrs': 
            sub_df = None
            directory = "prod/features/smrs/final_fts/group=group_/"
            for group in ['group_1', 'group_2', 'group_3']: 
                sub_df_1 = utils.load_from_s3(spark, directory.replace("group_", group)).where(f"date = {run_date_str}")
                sub_df = sub_df_1 if sub_df is None else sub_df.join(sub_df_1, "msisdn", 'outer')

        elif table == 'device': 
            directory = feature_destination[table]['destination'].replace("date=", "")
            device = utils.load_from_s3(spark, directory).where(f"date = {run_date_str}")
            tac_mapping = utils.load_from_s3(spark, feature_destination['tac_mapping']['destination'].replace("date=", "")).where(f"date = {run_date_str}")
            
            sub_df = tac_mapping.join(device, ['tac'], "left")

        else:
            directory = feature_destination[table]['destination'].replace("date=", "")
            sub_df = utils.load_from_s3(spark, directory).where(f"date = {run_date_str}")
        
        # select cols
        picked_cols = lst_features + ["msisdn"]
        sub_df = sub_df.select(*picked_cols)

        # combine data
        if run_mode == 'prod': 
            sub_df = sub_df.where("left(msisdn, 4) = '0084' and length(msisdn) = 13")
        df = sub_df if df is None else df.join(sub_df, "msisdn", "outer")
    
    return df 

def run_merge(model_code, client_name = 'baseline'): 

    # env variable
    args = utils.process_args_to_dict(sys.argv[1])
    python_file = args['python_file']
    dag_name = args['dag_name']
    run_mode = args['run_mode']
    skip_dags = args['skip_dags'].split(",")
    rm_prefix_skip_dag = [dag.replace("BLI_DAG_", "").strip() for dag in skip_dags] # BLI_DAG_cs_v2_202505_mc => cs_v2_202505_mc
    dt_from = datetime.strptime(args['dt_from'], "%Y%m%d") + timedelta(days=1)
    dt_to = datetime.strptime(args['dt_to'], "%Y%m%d") 

    if f"{model_code}_{client_name}".lower() in rm_prefix_skip_dag:
        utils.send_msg_to_telegram(f"SKIP DAG: {dag_name} | node: {python_file} | task : merge | run mode: {run_mode}")
        return
    
    # init spark and define path
    spark = utils.create_spark_instance()
    model_code_client = f"model_code={model_code}/client={client_name}"
    
    # load config
    feature_dict = utils.load_pickle(f"models/{model_code_client}/feature_dict.pkl")
    common_config = utils.load_config("configs", "common.yaml")

    while dt_from <= dt_to:
        run_date = dt_from.strftime('%Y%m%d')
        
        if dt_from.weekday() == 0:   
            directory = common_config['model_path'][model_code] + f"client={client_name}/merged_fts/date={run_date}"

            try: 
                utils.load_from_s3(spark, directory)
                utils.send_msg_to_telegram(f"INGORE: {dag_name} | node: {python_file} | task : merge | date: {run_date} | run mode: {run_mode}")
            except: 
                merge_df = load_and_merge_dataset(spark, feature_dict, common_config, run_date, run_mode)

                # save merged data
                utils.save_to_s3(merge_df, directory)
                utils.send_msg_to_telegram(f"FINISH: {dag_name} | node: {python_file} | task : merge | date: {run_date} | run mode: {run_mode}")

        dt_from = dt_from + relativedelta(days=1)