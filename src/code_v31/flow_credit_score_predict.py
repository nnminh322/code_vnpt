import os
from config_edit import config
import utils
import site
import sys
from pyspark.sql import functions as F, types as T

os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

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

def make_predictions(sc, model, df, cat_fts, transform_ft, opt_obj, return_fts=False):
    # Apply transformations
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
        predictions = clf.value.predict_proba(X)[:, 1]
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

final_feature=['sub_balance_pct_day_20k_to_50k_l4w_vs_l12w',
 'f_blueinfo_vascdr_brandname_meta_least-frequent-value_SENDER**Filter=full_T-2-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_TOT_SMS_ONNET_OG_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_RCHRG_PRMTN_AMT*s3_file_date*Filter=full_T-2-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_RCHRG_PRMTN_AMT*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_ocs_crs_usage_max_totalcost**Filter=CHARGINGCATEGORYNAME==DATA_T-3-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_DISC_AMT*s3_file_date*Filter=full_T-3-acc',
 'f4694_blueinfo_voice_msc_ratio_CALL_DURATION*s3_file_date*Filter=ROA_T-2',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_ELOAD_EVT_CNT*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_TOT_VOICE_ONNET_OG_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_RCV_AMT*s3_file_date*Filter=full_T-2-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_RCHRG_PRMTN_AMT*s3_file_date*Filter=full_T-1-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_VOICE_FREE_USG*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_ocs_crs_usage_ratio_totalcost**Filter=CHARGINGCATEGORYNAME==SMS_T-3-acc',
 'f169_blueinfo_ocs_air_min_reillamount**Filter=full_T-3',
 'f4493_blueinfo_voice_msc_std_CALL_DURATION*s3_file_date*Filter=ROA_T-1',
 'device_32_bit',
 'f_blueinfo_ocs_crs_usage_sum_totalcost**Filter=CHARGINGCATEGORYNAME==DATA_T-1-acc',
 'sub_balance_pct_day_ge_100k_l4w_vs_l12w',
 'f_blueinfo_ocs_crs_usage_avg_totalcost**Filter=CHARGINGCATEGORYNAME==DATA_T-2-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_VOICE_ONNET_OG_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_TOT_VOICE_ONNET_OG_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_ocs_crs_usage_ratio_totalcost**Filter=CHARGINGCATEGORYNAME==VOICE_T-3-acc',
 'f_blueinfo_ocs_crs_usage_avg_totalcost**Filter=CHARGINGCATEGORYNAME==SMS_T-1-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_SMS_CNT*s3_file_date*Filter=full_T-2-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_VOICE_FREE_USG*s3_file_date*Filter=full_T-1-acc',
 'device_lpwan',
 'f4477_blueinfo_voice_msc_avg_CALL_DURATION*s3_file_date*Filter=MOC_T-1',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_DISC_AMT*s3_file_date*Filter=full_T-2-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_SMS_OFFNET_OG_CNT*s3_file_date*Filter=full_T-2-acc',
 'f4446_blueinfo_vascloud_da_std_value**Filter=full_T-2',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_RCHRG_PRMTN_AMT*s3_file_date*Filter=full_T-3-acc',
 'f4479_blueinfo_voice_msc_min_CALL_DURATION*s3_file_date*Filter=MOC_T-1',
 'f_blueinfo_ocs_crs_usage_count_totalcost**Filter=CHARGINGCATEGORYNAME==DATA_T-3-acc',
 'f_blueinfo_vascdr_brandname_meta_least-frequent-percent_SENDER**Filter=finance_T-3-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_RCHRG_PRMTN_AMT*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_ocs_crs_usage_count_totalcost**Filter=CHARGINGCATEGORYNAME==VOICE_T-2-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_TOT_DISC_AMT*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_vascdr_brandname_meta_least-frequent-percent_SENDER**Filter=finance_T-1-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_SMS_ONNET_OG_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_ELOAD_AMT*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_ccbs_spi_3g_subs_least-frequent-percent_last_action**Filter=full_T-3-acc',
 'f_blueinfo_ocs_crs_usage_max_totalcost**Filter=CHARGINGCATEGORYNAME==VOICE_T-2-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_RCV_AMT*s3_file_date*Filter=full_T-2-acc',
 'f_blueinfo_ocs_crs_usage_avg_totalcost**Filter=CHARGINGCATEGORYNAME==DATA_T-3-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_SMS_OFFNET_OG_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
 'f4476_blueinfo_voice_msc_sum_CALL_DURATION*s3_file_date*Filter=MOC_T-1',
 'f_blueinfo_ocs_crs_usage_count_totalcost**Filter=CHARGINGCATEGORYNAME==IMS_T-1-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_SMS_ONNET_IC_CNT*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_ocs_crs_usage_ratio_totalcost**Filter=CHARGINGCATEGORYNAME==VoLTE_T-2-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_SMS_RVN_AMT*s3_file_date*Filter=full_T-2-acc',
 'device_chipset_family',
 'f_blueinfo_vascdr_brandname_meta_least-frequent-percent_SENDER**Filter=finance_T-2-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_VOICE_USG*s3_file_date*Filter=full_T-1-acc',
 'Age_group',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_SMS_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
 'f_blueinfo_ccbs_spi_3g_subs_most-frequent-percent_service_code**Filter=full_T-3-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_VOICE_OFFNET_IC_USG*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_ocs_crs_usage_sum_totalcost**Filter=CHARGINGCATEGORYNAME==VOICE_T-3-acc',
 'f133_blueinfo_ocs_air_min_reillamount**Filter=full_T-1',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_TOT_VOICE_OFFNET_OG_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_ELOAD_AMT*s3_file_date*Filter=full_T-2-acc',
 'sub_balance_pct_day_50k_to_100k_l4w',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_VOICE_OFFNET_IC_USG*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_SMS_CNT*s3_file_date*Filter=full_T-1-acc',
 'f175_blueinfo_ocs_air_min_reillamount**Filter=module==RefillRecord_T-3',
 'f4480_blueinfo_voice_msc_max_CALL_DURATION*s3_file_date*Filter=MOC_T-1',
 'device_gpu_family',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_VOICE_ONNET_OG_RVN_AMT*s3_file_date*Filter=full_T-2-acc',
 'f4447_blueinfo_vascloud_da_sum_value**Filter=full_T-3',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_ELOAD_AMT*s3_file_date*Filter=full_T-2-acc',
 'f151_blueinfo_ocs_air_min_reillamount**Filter=full_T-2',
 'f4515_blueinfo_voice_msc_min_CALL_DURATION*s3_file_date*Filter=MOC_T-2',
 'sub_balance_pct_day_5k_to_10k_l4w',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_VOICE_OFFNET_OG_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_RCV_AMT*s3_file_date*Filter=full_T-1-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_TOT_VOICE_OFFNET_OG_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_ocs_crs_usage_ratio_totalcost**Filter=CHARGINGCATEGORYNAME==SERVICE_CHARGING (SCAP)_T-2-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_TOT_DISC_AMT*s3_file_date*Filter=full_T-1-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_ELOAD_EVT_CNT*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_TOT_DISC_AMT*s3_file_date*Filter=full_T-1-acc',
 'f_blueinfo_vascdr_brandname_meta_least-frequent-percent_SENDER**Filter=full_T-3-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_SMS_ONNET_OG_CNT*s3_file_date*Filter=full_T-2-acc',
 'f141_blueinfo_ocs_air_std_reillamount**Filter=module==RefillRecord_T-1',
 'f3819_blueinfo_ccbs_ct_no_count_THUE**Filter=full_T-3',
 'f3813_blueinfo_ccbs_ct_no_count_NOGOC**Filter=full_T-3',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_VOICE_ONNET_IC_USG*s3_file_date*Filter=full_T-2-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_TOT_VOICE_OFFNET_OG_RVN_AMT*s3_file_date*Filter=full_T-2-acc',
 'f35_blueinfo_ggsn_max_DATA_VOLUME_UPLINK_MB**Filter=full_T-2',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_VOICE_ONNET_OG_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_VAS_RVN_AMT*s3_file_date*Filter=full_T-2-acc',
 'f_blueinfo_ocs_crs_usage_avg_totalcost**Filter=CHARGINGCATEGORYNAME==VOICE_T-2-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_RCV_AMT*s3_file_date*Filter=full_T-3-acc',
 'f239_blueinfo_ocs_air_ratio_reillamount**Filter=module==RefillRecord_T-2',
 'f131_blueinfo_ocs_air_avg_reillamount**Filter=full_T-1',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_TOT_SMS_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_ocs_crs_usage_sum_totalcost**Filter=CHARGINGCATEGORYNAME==VoLTE_T-2-acc',
 'f_blueinfo_vascdr_brandname_meta_most-frequent-value_SENDER**Filter=full_T-3-acc',
 'f4692_blueinfo_voice_msc_ratio_CALL_DURATION*s3_file_date*Filter=MOC_T-2',
 'f4558_blueinfo_voice_msc_max_CALL_DURATION*s3_file_date*Filter=MTC_T-3',
 'f4761_blueinfo_voice_volte_min_call_duration*s3_file_date*Filter=MOC_T-2',
 'f4551_blueinfo_voice_msc_min_CALL_DURATION*s3_file_date*Filter=MOC_T-3',
 'f_blueinfo_ocs_crs_usage_avg_totalcost**Filter=CHARGINGCATEGORYNAME==SMS_T-2-acc',
 'f_blueinfo_vascdr_brandname_meta_most-frequent-percent_SENDER**Filter=finance_T-3-acc',
 'f4484_blueinfo_voice_msc_count_CALL_DURATION*s3_file_date*Filter=MTC_T-1',
 'f4486_blueinfo_voice_msc_max_CALL_DURATION*s3_file_date*Filter=MTC_T-1',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_VOICE_ONNET_IC_USG*s3_file_date*Filter=full_T-3-acc',
 'f5_blueinfo_ggsn_max_DURATION_MINUTES**Filter=full_T-1',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_TOT_DATA_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_ocs_crs_usage_count_totalcost**Filter=CHARGINGCATEGORYNAME==VoLTE_T-2-acc',
 'f4557_blueinfo_voice_msc_min_CALL_DURATION*s3_file_date*Filter=MTC_T-3',
 'f4794_blueinfo_voice_volte_sum_call_duration*s3_file_date*Filter=MOC_T-3',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_TOT_SMS_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
 'f4798_blueinfo_voice_volte_max_call_duration*s3_file_date*Filter=MOC_T-3',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_TOT_DISC_AMT*s3_file_date*Filter=full_T-2-acc',
 'f6_blueinfo_ggsn_std_DURATION_MINUTES**Filter=full_T-1',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_TOT_DATA_CHRGD_AMT*s3_file_date*Filter=full_T-1-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_TOT_RC_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_VOICE_ONNET_IC_USG*s3_file_date*Filter=full_T-3-acc',
 'f4430_blueinfo_vascdr_utn_credit_log_ratio_CREDIT_AMOUNT**Filter=full_T-2',
 'f4431_blueinfo_vascdr_utn_credit_log_ratio_CREDIT_AMOUNT**Filter=full_T-3',
 'device_ram_category',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_TOT_DATA_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_RCV_AMT*s3_file_date*Filter=full_T-3-acc',
 'f4797_blueinfo_voice_volte_min_call_duration*s3_file_date*Filter=MOC_T-3',
 'f3_blueinfo_ggsn_count_DURATION_MINUTES**Filter=full_T-1',
 'f15_blueinfo_ggsn_count_DATA_VOLUME_UPLINK_MB**Filter=full_T-1',
 'Sex',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_VOICE_ONNET_OG_USG*s3_file_date*Filter=full_T-2-acc',
 'f4521_blueinfo_voice_msc_min_CALL_DURATION*s3_file_date*Filter=MTC_T-2',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_VOICE_ONNET_IC_USG*s3_file_date*Filter=full_T-2-acc',
 'f4767_blueinfo_voice_volte_min_call_duration*s3_file_date*Filter=MTC_T-2',
 'f135_blueinfo_ocs_air_std_reillamount**Filter=full_T-1',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_TOT_DISC_AMT*s3_file_date*Filter=full_T-3-acc',
 'f4795_blueinfo_voice_volte_avg_call_duration*s3_file_date*Filter=MOC_T-3',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_DATA_USG*s3_file_date*Filter=full_T-1-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_VOICE_ONNET_OG_USG*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_DATA_BYTES_RCVD*s3_file_date*Filter=full_T-1-acc',
 'f238_blueinfo_ocs_air_ratio_reillamount**Filter=full_T-2',
 'f4482_blueinfo_voice_msc_sum_CALL_DURATION*s3_file_date*Filter=MTC_T-1',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_DATA_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_VOICE_OFFNET_IC_USG*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_ocs_crs_usage_std_totalcost**Filter=CHARGINGCATEGORYNAME==SMS_T-1-acc',
 'f4485_blueinfo_voice_msc_min_CALL_DURATION*s3_file_date*Filter=MTC_T-1',
 'f_blueinfo_vascdr_brandname_meta_most-frequent-percent_SENDER**Filter=full_T-3-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_VOICE_ONNET_OG_USG*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_VOICE_OFFNET_OG_USG*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_RCV_AMT*s3_file_date*Filter=full_T-2-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_VOICE_OFFNET_OG_USG*s3_file_date*Filter=full_T-1-acc',
 'f4404_blueinfo_vascdr_utn_credit_log_max_CREDIT_AMOUNT**Filter=full_T-2',
 'f4401_blueinfo_vascdr_utn_credit_log_avg_CREDIT_AMOUNT**Filter=full_T-2',
 'f4194_blueinfo_ccbs_cv207_least-frequent-item_ma_tinh**Filter=full_T-2',
 'f_blueinfo_vascdr_brandname_meta_most-frequent-percent_SENDER**Filter=full_T-1-acc',
 'f1_blueinfo_ggsn_sum_DURATION_MINUTES**Filter=full_T-1',
 'f4731_blueinfo_voice_volte_min_call_duration*s3_file_date*Filter=MTC_T-1',
 'f4938_blueinfo_voice_volte_ratio_call_duration*s3_file_date*Filter=MOC_T-2',
 'f4187_blueinfo_ccbs_cv207_least-frequent-item_ma_tinh**Filter=full_T-1',
 'f4699_blueinfo_voice_msc_ratio_CALL_DURATION*s3_file_date*Filter=MTC_T-3',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_VOICE_ONNET_IC_USG*s3_file_date*Filter=full_T-3-acc',
 'f4732_blueinfo_voice_volte_max_call_duration*s3_file_date*Filter=MTC_T-1',
 'f4764_blueinfo_voice_volte_sum_call_duration*s3_file_date*Filter=MTC_T-2',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_VOICE_OFFNET_OG_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_ocs_crs_usage_avg_totalcost**Filter=CHARGINGCATEGORYNAME==SERVICE_CHARGING (SCAP)_T-1-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_TOT_DATA_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
 'f32_blueinfo_ggsn_avg_DATA_VOLUME_UPLINK_MB**Filter=full_T-2',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_VOICE_BLLD_USG*s3_file_date*Filter=full_T-1-acc',
 'device_internal_storage',
 'f4766_blueinfo_voice_volte_count_call_duration*s3_file_date*Filter=MTC_T-2',
 'f4204_blueinfo_ccbs_cv207_most-frequent-value_ma_tinh**Filter=full_T-3',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_TOT_DISC_AMT*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_ocs_crs_usage_avg_totalcost**Filter=CHARGINGCATEGORYNAME==SERVICE_CHARGING (SCAP)_T-2-acc',
 'f_blueinfo_vascdr_brandname_meta_most-frequent-item_SENDER**Filter=finance_T-1-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_TOT_VAS_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
 'f166_blueinfo_ocs_air_sum_reillamount**Filter=full_T-3',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_VOICE_USG*s3_file_date*Filter=full_T-3-acc',
 'f4944_blueinfo_voice_volte_ratio_call_duration*s3_file_date*Filter=MOC_T-3',
 'f130_blueinfo_ocs_air_sum_reillamount**Filter=full_T-1',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_TOT_VOICE_RVN_AMT*s3_file_date*Filter=full_T-2-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_SMS_BLLD_USG_CNT*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_VOICE_BLLD_USG*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_vascdr_brandname_meta_unique_SENDER**Filter=finance_T-3-acc',
 'f_blueinfo_vascdr_brandname_meta_unique_SENDER**Filter=full_T-2-acc',
 'f_blueinfo_ocs_crs_usage_count_totalcost**Filter=CHARGINGCATEGORYNAME==SERVICE_CHARGING (SCAP)_T-2-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_VOICE_OFFNET_IC_USG*s3_file_date*Filter=full_T-2-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_VAS_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_VOICE_USG*s3_file_date*Filter=full_T-3-acc',
 'f12_blueinfo_ggsn_std_DATA_VOLUME_DOWNLINK_MB**Filter=full_T-1',
 'f22_blueinfo_ggsn_min_DURATION_MINUTES**Filter=full_T-2',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_SMS_OFFNET_OG_CNT*s3_file_date*Filter=full_T-2-acc',
 'sub_balance_pct_day_lt_10k_l1w_vs_l4w',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_RC_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
 'f240_blueinfo_ocs_air_ratio_reillamount**Filter=module==AdjustmentRecord_T-2',
 'f40_blueinfo_ggsn_min_DURATION_MINUTES**Filter=full_T-3',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_VOICE_OFFNET_OG_USG*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_VOICE_FREE_USG*s3_file_date*Filter=full_T-2-acc',
 'f_blueinfo_vascdr_brandname_meta_most-frequent-item_SENDER**Filter=finance_T-3-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_DATA_USG*s3_file_date*Filter=full_T-3-acc',
 'f4939_blueinfo_voice_volte_ratio_call_duration*s3_file_date*Filter=MTC_T-2',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_DATA_EVT_CNT*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_VOICE_USG*s3_file_date*Filter=full_T-2-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_TOT_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_max_TOT_VOICE_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
 'f113_blueinfo_ggsn_ratio_DATA_VOLUME_DOWNLINK_MB**Filter=full_T-3',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_VOICE_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
 'f161_blueinfo_ocs_air_avg_reillamount**Filter=module==AdjustmentRecord_T-2',
 'f164_blueinfo_ocs_air_max_reillamount**Filter=module==AdjustmentRecord_T-2',
 'f_blueinfo_ocs_crs_usage_std_totalcost**Filter=CHARGINGCATEGORYNAME==SERVICE_CHARGING (SCAP)_T-3-acc',
 'sub_balance_pct_day_lt_5k_l4w_vs_l12w',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_TOT_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_TOT_VOICE_RVN_AMT*s3_file_date*Filter=full_T-2-acc',
 'f160_blueinfo_ocs_air_sum_reillamount**Filter=module==AdjustmentRecord_T-2',
 'f_blueinfo_ocs_crs_usage_std_totalcost**Filter=CHARGINGCATEGORYNAME==SERVICE_CHARGING (SCAP)_T-2-acc',
 'f_blueinfo_ocs_crs_usage_count_totalcost**Filter=CHARGINGCATEGORYNAME==_T-3-acc',
 'sub_balance_pct_day_lt_5k_l1w_vs_l4w',
 'f_blueinfo_vascdr_brandname_meta_least-frequent-item_SENDER**Filter=finance_T-3-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_SMS_OFFNET_OG_CNT*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_SMS_ONNET_IC_CNT*s3_file_date*Filter=full_T-1-acc',
 'f4195_blueinfo_ccbs_cv207_most-frequent-item_ma_tinh**Filter=full_T-2',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_SMS_OFFNET_IC_CNT*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_max_SMS_ONNET_IC_CNT*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_BLLD_AMT*s3_file_date*Filter=full_T-1-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_TOT_VAS_RVN_AMT*s3_file_date*Filter=full_T-2-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_DATA_BYTES_RCVD*s3_file_date*Filter=full_T-2-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_DATA_EVT_CNT*s3_file_date*Filter=full_T-2-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_VOICE_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_TOT_BLLD_AMT*s3_file_date*Filter=full_T-1-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_BLLD_AMT*s3_file_date*Filter=full_T-2-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_TOT_BLLD_AMT*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_SMS_CNT*s3_file_date*Filter=full_T-1-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_TOT_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
 'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_SMS_OFFNET_IC_CNT*s3_file_date*Filter=full_T-3-acc']
missing=['sub_balance_pct_day_5k_to_10k_l4w',
 'sub_balance_pct_day_50k_to_100k_l4w',
 'sub_balance_pct_day_lt_5k_l1w_vs_l4w',
 'sub_balance_pct_day_lt_5k_l4w_vs_l12w',
 'sub_balance_pct_day_lt_10k_l1w_vs_l4w',
 'sub_balance_pct_day_20k_to_50k_l4w_vs_l12w',
 'sub_balance_pct_day_ge_100k_l4w_vs_l12w',
 'device_internal_storage']
cats=['device_32_bit',
 'device_lpwan',
 'device_chipset_family',
 'Age_group',
 'device_gpu_family',
 'device_ram_category',
 'f4194_blueinfo_ccbs_cv207_least-frequent-item_ma_tinh**Filter=full_T-2',
 'f4187_blueinfo_ccbs_cv207_least-frequent-item_ma_tinh**Filter=full_T-1',
 'f_blueinfo_vascdr_brandname_meta_most-frequent-item_SENDER**Filter=finance_T-1-acc',
 'f_blueinfo_vascdr_brandname_meta_most-frequent-item_SENDER**Filter=finance_T-3-acc',
 'f_blueinfo_vascdr_brandname_meta_least-frequent-item_SENDER**Filter=finance_T-3-acc',
 'f4195_blueinfo_ccbs_cv207_most-frequent-item_ma_tinh**Filter=full_T-2']
other_info=['msisdn', 'date']


### main
ALL_MONTHS = config.ALL_MONTHS
ALL_MONTHS.sort(reverse = True)
fix_dates = config.fix_date
fix_dates.sort(reverse = True)
run_mode = config.run_mode
utils.install_package()

spark = utils.create_spark_instance()
sc = spark.sparkContext

import subprocess
cmd = ["pip", "show", "catboost"]
result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
print(result.stdout)
import catboost
import optbinning

df = spark.read.parquet(config.cs_data_dir)

## rename column
not_exists  = [
'f4694_blueinfo_voice_msc_ratio_CALL_DURATION*s3_file_date*Filter=ROA_T-2',
 'f169_blueinfo_ocs_air_min_reillamount**Filter=full_T-3',
 'f4493_blueinfo_voice_msc_std_CALL_DURATION*s3_file_date*Filter=ROA_T-1',
 'f175_blueinfo_ocs_air_min_reillamount**Filter=module==RefillRecord_T-3',
 'f151_blueinfo_ocs_air_min_reillamount**Filter=full_T-2',
 'f4515_blueinfo_voice_msc_min_CALL_DURATION*s3_file_date*Filter=MOC_T-2',
 'f141_blueinfo_ocs_air_std_reillamount**Filter=module==RefillRecord_T-1',
 'f239_blueinfo_ocs_air_ratio_reillamount**Filter=module==RefillRecord_T-2',
 'f4692_blueinfo_voice_msc_ratio_CALL_DURATION*s3_file_date*Filter=MOC_T-2',
 'f4558_blueinfo_voice_msc_max_CALL_DURATION*s3_file_date*Filter=MTC_T-3',
 'f4551_blueinfo_voice_msc_min_CALL_DURATION*s3_file_date*Filter=MOC_T-3',
 'f4484_blueinfo_voice_msc_count_CALL_DURATION*s3_file_date*Filter=MTC_T-1',
 'f4486_blueinfo_voice_msc_max_CALL_DURATION*s3_file_date*Filter=MTC_T-1',
 'f4557_blueinfo_voice_msc_min_CALL_DURATION*s3_file_date*Filter=MTC_T-3',
 'f4521_blueinfo_voice_msc_min_CALL_DURATION*s3_file_date*Filter=MTC_T-2',
 'f238_blueinfo_ocs_air_ratio_reillamount**Filter=full_T-2',
 'f4482_blueinfo_voice_msc_sum_CALL_DURATION*s3_file_date*Filter=MTC_T-1',
 'f4485_blueinfo_voice_msc_min_CALL_DURATION*s3_file_date*Filter=MTC_T-1',
 'f4699_blueinfo_voice_msc_ratio_CALL_DURATION*s3_file_date*Filter=MTC_T-3',
 'f166_blueinfo_ocs_air_sum_reillamount**Filter=full_T-3',
 'f240_blueinfo_ocs_air_ratio_reillamount**Filter=module==AdjustmentRecord_T-2',
 'f161_blueinfo_ocs_air_avg_reillamount**Filter=module==AdjustmentRecord_T-2',
 'f164_blueinfo_ocs_air_max_reillamount**Filter=module==AdjustmentRecord_T-2',
 'f160_blueinfo_ocs_air_sum_reillamount**Filter=module==AdjustmentRecord_T-2']

not_exists_dct = {}
for col1 in not_exists: 
    old_col = col1.split("blueinfo")[1:]
    for col2 in df.columns: 
        new_col = col2.split("blueinfo")[1:]
        if old_col == new_col: 
            not_exists_dct[col2] = col1
            
for k, v in not_exists_dct.items(): 
    df = df.withColumnRenamed(k, v)
###############

res_num= utils.load_pickle_file(config.cs_binning_path)
model= utils.load_pickle_file(config.cs_model_path)

if run_mode == 'backtest':
    df = df.select(final_feature+other_info) 
    df = df.dropDuplicates()
    df = df.where(f"date <= {ALL_MONTHS[0]}{fix_dates[0]}")

    data=spark.read.parquet(config.cs_data_bt_dir)
    data=data.select('PHONE_NUMBER','SNAPSHOT', 'APPLICATION_DATE')
    df=data.join(df,[data.PHONE_NUMBER==df.msisdn],how='left')

    distinct_dates = df.select("date").distinct().toPandas()['date'].to_list()

    for date in distinct_dates: 
        print(f"Predict: {date}")
        sub_df = df.where(f"date = '{date}'").dropDuplicates(subset = ["msisdn"])
        if sub_df.count() == 0: 
            continue
        
        df_proba = make_predictions(spark.sparkContext, model, sub_df, cats, missing, res_num)
        df_comm_score = transform_to_com_score(df_proba)
        df_comm_score.write.mode('overwrite').parquet(f"{config.cs_output_path}/date={date}")
    df_load = spark.read.parquet(f"{config.cs_output_path}/")
    df_load = df_load.withColumn("date", F.to_date(F.col("date").cast("string"),  "yyyyMMdd"))

    # join y_label
    data=spark.read.parquet(config.cs_data_bt_dir)
    data=data.select('PHONE_NUMBER','SNAPSHOT', 'APPLICATION_DATE')
    df_load=data.join(df_load,[data.PHONE_NUMBER==df_load.msisdn, data.SNAPSHOT==df_load.date],how='left')
    df_load.write.mode('overwrite').parquet(config.cs_output_path.replace("cs_predict/", "cs_predict_final/"))
else: 
    df = df.select(final_feature+['msisdn'])
    df_proba = make_predictions(spark.sparkContext, model, df, cats, missing, res_num)
    df_comm_score = transform_to_com_score(df_proba)
    df_comm_score.write.mode('overwrite').parquet(config.cs_output_path)