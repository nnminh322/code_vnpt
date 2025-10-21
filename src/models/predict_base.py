import sys
import site
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta, MO
from src.utils import common as utils 
from pyspark.sql.window import Window
from pyspark.sql import functions as F, types as T

# install package
utils.install_package()

user_site = site.getusersitepackages()
if user_site not in sys.path: 
    sys.path.append(user_site)

def install_package(users=True):
    import subprocess
    import sys
    file_path = ["scipy==1.8.1", "optbinning==0.20.1", "catboost==1.2.7", "pandas==1.4.4"]

    if users:
        sys.path.append("/.local/lib/python3.9/site-packages")
        cmd = [sys.executable, "-m", "pip", "install", "--user", "--no-cache-dir", "--proxy", "http://10.144.13.144:3129"] + file_path
    else: 
        cmd = [sys.executable, "-m", "pip", "install", "--no-cache-dir", "--proxy", "http://10.144.13.144:3129"] + file_path

    try:
        result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return ["Successfully install packages"]
    except subprocess.CalledProcessError as e:
        print(f"Error when install packages", e)
        print(e.stdout)
        print(e.stderr)

def binning_expr(column, binning_table, handle_null=False):
    """
    Builds a CASE WHEN SQL expression from the binning table.
    Parameters:
        column (str): Name of the column to transform.
        binning_table (list of dict): Binning table containing 'bin_range' and 'woe' values.
        handle_null = True --> return WoE, else 0
    Returns:
        str: CASE WHEN SQL expression.
    """
    binning_table = binning_table[binning_table.index!="Totals"]
    # Start building the CASE WHEN expression
    case_expr = "CASE"
    for index, bin_info in binning_table.iterrows():
        bin_range = bin_info["bin"]
        woe_value = bin_info["woe"]

        # Handle 'Missing' explicitly
        if bin_range == "Missing":
            if handle_null:
                case_expr += f" WHEN {column} IS NULL THEN {woe_value}"
            else:
                case_expr += f" WHEN {column} IS NULL THEN 0"
        elif bin_range == "Special":
            # Example: add a specific condition for 'Special' if needed
            continue  # Skip or define specific logic for 'Special'
        else:
            # Parse the range
            try:
                lower, upper = bin_range.strip("()[]").split(", ")
                lower = float(lower) if lower != '-inf' else -float("inf")
                upper = float(upper) if upper != 'inf' else float("inf")
            except ValueError:
                raise ValueError(f"Unexpected bin range format: {bin_range}")

            if lower == -float("inf"):
                case_expr += f" WHEN {column} < {upper} THEN {woe_value}"
            elif upper == float("inf"):
                case_expr += f" WHEN {column} >= {lower} THEN {woe_value}"
            else:
                case_expr += f" WHEN {column} >= {lower} AND {column} < {upper} THEN {woe_value}"
    case_expr += " ELSE 0 END"

    return case_expr

def opt_binning_num_transform(df, ft_list, opt_obj):

    for feature in ft_list:
        # if feature in df
        if feature in df.columns:
            # get binning table
            binning_table = opt_obj[feature].binning_table.build()
            
            # lower case column name
            binning_table.columns = [x.lower() for x in binning_table.columns]
            
            # create transform statement
            bin_expr = binning_expr(feature, binning_table)

            # transform
            df = df.withColumn(feature, F.expr(bin_expr).cast('float'))
        else:
            print("column not found: ", feature)

    return df

def make_predictions(sc, model, df, cat_fts, transform_ft, opt_obj, model_type, return_fts=False):
    # Apply transformations and fill strategy
    if opt_obj:
        df = opt_binning_num_transform(df, transform_ft, opt_obj)

    # Fill NaN for categorical columns
    df = df.fillna(value={col: "X" for col in cat_fts})

    # Ensure feature names match the model's feature names
    expected_features = model.feature_names_

    # Broadcast the model to Spark executors
    clf = sc.broadcast(model)

    # Define the prediction UDF
    @F.pandas_udf(returnType=T.DoubleType())
    def predict(*cols):
        install_package(users=False)
        import catboost
        import pandas as pd

        # Combine columns into a Pandas DataFrame
        X = pd.concat([pd.Series(col) for col in cols], axis=1)
        X.columns = expected_features
        # Predict probabilities
        predictions = clf.value.predict(X, prediction_type="Probability")[:, 1]
        return pd.Series(predictions)


    # Apply prediction UDF and return results
    df = df.withColumn("predict_proba", predict(*[df[col] for col in expected_features]))
    
    # select column
    if return_fts:
        df = df.select("msisdn", "predict_proba", *expected_features)
    else:
        df = df.select("msisdn", "predict_proba")

    return df

def logit(proba):
    import math
    return math.log(proba / (1 - proba))

def neg_logit(proba):
    return -logit(proba)

logit_udf = F.udf(logit, T.DoubleType())
neg_logit_udf = F.udf(neg_logit, T.DoubleType())

def scale_to_new_range(df, range_lb=300, range_ub=850, target_column="credit_score"):
    score_lb, score_ub = list(df.agg(F.min(target_column), F.max(target_column)).collect()[0])
    scale_factor = float(range_ub - range_lb) / (score_ub - score_lb)
    return df.withColumn(
        target_column,
        F.floor(F.round((F.col(target_column) - F.lit(score_lb)) * scale_factor)) + range_lb)

def transform_to_com_score(
    df, ori_column="predict_proba", target_column="credit_score",
    cscore_lb=300, cscore_up=850, pct_lb=0.01, pct_ub=0.99, relativeError=0.000001):
    mod_column = "mod_{0}".format(ori_column)
    df = df.withColumn(mod_column, F.col(ori_column))   

    if pct_lb != 0:
        proba_lb = df.stat.approxQuantile(mod_column, [pct_lb], relativeError)[0]
        df = df.withColumn(
            mod_column, F.when(F.col(mod_column) > F.lit(proba_lb), F.col(mod_column)).otherwise(proba_lb))

    if pct_ub != 1:
        proba_ub = df.stat.approxQuantile(mod_column, [pct_ub], relativeError)[0]
        df = df.withColumn(
            mod_column, F.when(F.col(mod_column) < F.lit(proba_ub), F.col(mod_column)).otherwise(proba_ub))
    df = df.withColumn(target_column, neg_logit_udf(mod_column))
    df = df.withColumn(target_column, F.pow(F.col(target_column), F.lit(1.3)))

    return scale_to_new_range(
        df,
        range_lb=cscore_lb,
        range_ub=cscore_up,
        target_column=target_column)

def predict(spark, model, df, cat_feature, num_features, res_num, model_type, run_date): 
    # df = df.select(cat_feature + num_features +['msisdn'])
    n_partitions = spark.sparkContext.defaultParallelism * 3
    df = df.repartition(n_partitions)
    
    df_proba = make_predictions(spark.sparkContext, model, df, cat_feature, num_features, res_num, model_type)
    
    if model_type == 'lg': 
        # split range
        win = Window.orderBy(F.asc("predict_proba"))
        df_final = df_proba.withColumn('lg_perct', F.ntile(100).over(win))
        df_final = df_final.withColumn('propensity_score', F.col('predict_proba') * 10000)
    else: 
        df_final = transform_to_com_score(df_proba)
    return df_final

def run_predict(model_code, model_pkl_name, client_name, feature_pkl_name, res_num = None, handlers = {}, external_params = None):
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
        utils.send_msg_to_telegram(f"SKIP DAG: {dag_name} | node: {python_file} | task : predict | run mode: {run_mode}")
        return
    
    # init spark and define path
    spark = utils.create_spark_instance()
    model_code_client = f"model_code={model_code}/client={client_name}"

    # load config
    model = utils.load_pickle(f"models/{model_code_client}/{model_pkl_name}.pkl")
    feature_dict = utils.load_pickle(f"models/{model_code_client}/{feature_pkl_name}.pkl")
    num_features = [f for s in feature_dict for f in feature_dict[s]['num']]
    cat_feature = [f for s in feature_dict for f in feature_dict[s]['cat']]
    common_config = utils.load_config("configs", "common.yaml")
    path = common_config['model_path'][model_code] + f"client={client_name}"

    # define params for handler function
    handler_params = {
        "num_features" : num_features,
        "cat_feature" : cat_feature,
    }
    if external_params: 
        handler_params.update(external_params)

    while dt_from <= dt_to:
        run_date = dt_from.strftime('%Y%m%d')
        
        if dt_from.weekday() == 0: 
            directory = path + f"/predict/date={run_date}"
            try: 
                utils.load_from_s3(spark, directory)
                utils.send_msg_to_telegram(f"INGORE: {dag_name} | node: {python_file} | task : predict | date: {run_date} | run mode: {run_mode}")
            except: 
                dataset = utils.load_from_s3(spark, path + f"/merged_fts/date={run_date}")
                dataset = dataset.where("sub_activated_flag == 1").drop("sub_activated_flag")
                
                # data processng
                for name, handler in handlers.items():
                    print(f"Data processing: {name} step")
                    if handler: 
                        dataset = handler(dataset, **handler_params)
                # predict
                model_type = model_code[:2]
                df_final = predict(spark, model, dataset, cat_feature, num_features, res_num, model_type, run_date)
                
                # save result
                utils.save_to_s3(df_final, directory)
                utils.send_msg_to_telegram(f"FINISH: {dag_name} | node: {python_file} | task : predict | date: {run_date} | run mode: {run_mode}")

        dt_from = dt_from + relativedelta(days=1)