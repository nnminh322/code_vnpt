from pyspark.sql import functions as F, types as T
from datetime import datetime
from dateutil.relativedelta import relativedelta, MO
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from .utils import feature_store_utils as fts_utils 
from src.utils import common as utils 

def gen_pn_bts_voice_fts(spark, bts_voice_dir, pn_fts_dir, run_date, pn_list):
    _pn = ', '.join(f"'{str(x)}'" for x in pn_list)
    df_fts = fts_utils.load_from_s3(spark, bts_voice_dir + "/final_fts")\
                .where(f"date = {run_date}")\
                .where(f"msisdn IN ({_pn})")
    
    ### agg to pn level fts
    exclude_list = ["msisdn", "date", "bts_id"]

    fts_cols = [x for x in df_fts.columns if x not in exclude_list]

    aggs = []
    aggs.extend(
        [F.sum(F.col(x)).alias("pn_" + x) for x in fts_cols]
    )
    
    # ratio_fts
    df_fts = df_fts.groupBy("msisdn").agg(*aggs)
    for freq_str in ['l1w', 'l4w', 'l12w', 'l24w']:
        call_in_ratio_str = f"pn_bts_voice_call_in_count_{freq_str} / pn_bts_voice_total_call_count_{freq_str}"
        call_out_ratio_str = f"pn_bts_voice_call_out_count_{freq_str} / pn_bts_voice_total_call_count_{freq_str}"

        sms_in_ratio_str = f"pn_bts_voice_sms_in_count_{freq_str} / pn_bts_voice_total_sms_count_{freq_str}"
        sms_out_ratio_str = f"pn_bts_voice_sms_out_count_{freq_str} / pn_bts_voice_total_sms_count_{freq_str}"

        total_call_ratio_str = f"pn_bts_voice_total_call_count_{freq_str} / pn_bts_voice_total_count_{freq_str}"
        total_sms_ratio_str = f"pn_bts_voice_total_sms_count_{freq_str} / pn_bts_voice_total_count_{freq_str}"
        
        df_fts = (
            df_fts.withColumn(f"pn_bts_voice_call_in_ratio_{freq_str}", F.expr(call_in_ratio_str))\
                  .withColumn(f"pn_bts_voice_call_out_ratio_{freq_str}", F.expr(call_out_ratio_str))\
                  .withColumn(f"pn_bts_voice_sms_in_ratio_{freq_str}", F.expr(sms_in_ratio_str))\
                  .withColumn(f"pn_bts_voice_sms_out_ratio_{freq_str}", F.expr(sms_out_ratio_str))\
                  .withColumn(f"pn_bts_voice_total_call_ratio_{freq_str}", F.expr(total_call_ratio_str))\
                  .withColumn(f"pn_bts_voice_total_sms_ratio_{freq_str}", F.expr(total_sms_ratio_str))
        )

    # Create lxw vs lyw features
    fts_names = [x for x in df_fts.columns if ("l1w" in x)&(x not in exclude_list)]
    lxw_list = ['l1w', 'l4w', 'l12w', 'l24w']

    for ft in fts_names:
        for i in range(0, len(lxw_list)):
            for j in range(i+2, len(lxw_list)):
                lxw = lxw_list[i]
                lyw = lxw_list[j]
                new_ft = ft[:ft.rfind('_')] + '_' + lxw + '_vs_' + lyw
                lxw_ft = ft[:ft.rfind('_')] + '_' + lxw
                lyw_ft = ft[:ft.rfind('_')] + '_' + lyw
                df_fts = df_fts.withColumn(new_ft, F.expr(f"{lxw_ft} / {lyw_ft}"))
    
    df_fts = df_fts.withColumn("run_time", F.lit(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    out_dir = f"{pn_fts_dir}/bts_voice/date={run_date}"
    fts_utils.save_to_s3(df_fts, out_dir, mode="append")