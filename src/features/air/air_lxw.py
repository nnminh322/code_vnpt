import sys
from pyspark.sql import functions as F, types as T
from datetime import datetime
from dateutil.relativedelta import relativedelta, MO
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from src.utils import common as utils 
from src.utils.job_quality_utils import status_check, run_create_feature


is_wk_str = "dayofweek(timestamps) in (1, 7)"
time_slot_str = """
    case
        when (hour < 6) or (hour >= 18) then 'nt'
        when (hour >= 6) and (hour < 12) then 'mo'
        when (hour >= 12) and (hour < 18) then 'af_ev'
    end
"""

time_slot_grp_str = """
   case
        when hour >= 6 and hour < 18 then 'dt'
        else 'nt'
    end  
"""
@status_check
def gen_air_lxw_fts(spark, table_name, bt_table_name, bt_msisdn_column, out_dir, run_date, run_mode, common_config, level):
    print("generating lxw fts for:", run_date)
    date_to = datetime.strptime(run_date, "%Y%m%d")
    
    # if level = weekly then generate l1w features
    # else generate lxw features --> difference directory
    for i in [1, 4, 12, 24]:
        # get date from / date to / freq_str
        date_from = date_to - relativedelta(days=i*7)   
        date_from_str = date_from.strftime("%Y%m%d")
        date_to_str = date_to.strftime("%Y%m%d")
        freq_str = f"l{i}w"
        print(f"--fts {freq_str} with level = {level}")
        
        query = f"""
            SELECT msisdn, reillamount/1000 as reillamount, origintimestamp, module, serviceclass, vouchergroup
            FROM {table_name} 
            WHERE 1=1
                AND s3_file_date >= '{date_from_str}' 
                AND s3_file_date < '{date_to_str}'
        """
        
        if i==1:
            if level == "weekly":
                df_air_raw = utils.spark_read_data_from_singlestore(spark, query).persist()
                df_air_raw.count()

                if run_mode == 'prod': 
                    df_air_raw = df_air_raw.where("left(msisdn, 4) = '0084' and length(msisdn) = 13")
     
                df_air = (
                    df_air_raw
                        .withColumn("timestamps", F.to_timestamp(F.col("origintimestamp").cast("string"), "yyyyMMddHHmmss"))
                        .withColumn("is_wk", F.expr(is_wk_str))
                        .withColumn("hour", F.date_format("timestamps", "hh"))
                        .withColumn("time_slot_grp", F.expr(time_slot_grp_str))
                )
                
                
                # write features
                air_fts = (
                    df_air.groupBy("msisdn")
                        .agg(
                                F.count("*").alias(f"air_balance_update_count_{freq_str}"),
                                F.sum(F.expr("case when is_wk = True then 1 end")).alias(f"air_balance_update_count_wk_{freq_str}"),
                                F.sum(F.expr("case when is_wk = False then 1 end")).alias(f"air_balance_update_count_wd_{freq_str}"),
                                F.sum(F.expr("case when time_slot_grp = 'dt' then 1 end")).alias(f"air_balance_update_count_dt_{freq_str}"),
                                F.sum(F.expr("case when time_slot_grp = 'nt' then 1 end")).alias(f"air_balance_update_count_nt_{freq_str}"),
                        
                                F.sum("reillamount").alias(f"air_balance_update_sum_{freq_str}"),
                                F.sum(F.expr("case when is_wk = True then reillamount end")).alias(f"air_balance_update_sum_wk_{freq_str}"),
                                F.sum(F.expr("case when is_wk = False then reillamount end")).alias(f"air_balance_update_sum_wd_{freq_str}"),
                                F.sum(F.expr("case when time_slot_grp = 'dt' then reillamount end")).alias(f"air_balance_update_sum_dt_{freq_str}"),
                                F.sum(F.expr("case when time_slot_grp = 'nt' then reillamount end")).alias(f"air_balance_update_sum_nt_{freq_str}"),

                                F.sum(F.expr("case when module = 'AdjustmentRecord' then reillamount end")).alias(f"air_adjustment_sum_{freq_str}"),
                                F.min(F.expr("case when module = 'AdjustmentRecord' then reillamount end")).alias(f"air_adjustment_min_{freq_str}"),
                                F.max(F.expr("case when module = 'AdjustmentRecord' then reillamount end")).alias(f"air_adjustment_max_{freq_str}"),
                                F.sum(F.expr("case when module = 'AdjustmentRecord' then 1 else 0 end")).alias(f"air_adjustment_count_{freq_str}"),
                                F.avg(F.expr("case when module = 'AdjustmentRecord' then reillamount end")).alias(f"air_adjustment_avg_{freq_str}"),
                                # F.stddev(F.expr("case when module = 'AdjustmentRecord' then reillamount end")).alias(f"air_adjustment_std_{freq_str}"),

                                F.sum(F.expr("case when module = 'RefillRecord' then reillamount end")).alias(f"air_refill_sum_{freq_str}"),
                                F.min(F.expr("case when module = 'RefillRecord' then reillamount end")).alias(f"air_refill_min_{freq_str}"),
                                F.max(F.expr("case when module = 'RefillRecord' then reillamount end")).alias(f"air_refill_max_{freq_str}"),
                                F.sum(F.expr("case when module = 'RefillRecord' then 1 else 0 end")).alias(f"air_refill_count_{freq_str}"),
                                F.avg(F.expr("case when module = 'RefillRecord' then reillamount end")).alias(f"air_refill_avg_{freq_str}"),
                                # F.stddev(F.expr("case when module = 'RefillRecord' then reillamount end")).alias(f"air_refill_std_{freq_str}"),

                                F.sum(F.expr("case when vouchergroup is not null and vouchergroup <> '' then 1 else 0 end")).alias(f"air_use_voucher_count_{freq_str}")
                            )
                )
            else: 
                continue
        else: 
            if level != "weekly":
                lower_fts = utils.load_from_s3(spark, out_dir + "/l1w").where(f"date > {date_from_str} and date <= {date_to_str}")
                old_suffix = "l1w"
                new_suffix = freq_str
                    
                # create features
                exclude_list = ["msisdn", "date"]
                aggs = []
                fts_cols = [x for x in lower_fts.columns if x not in exclude_list]
                
                aggs.extend(
                    [F.sum(F.col(x)).alias(x.replace(old_suffix, new_suffix)) for x in fts_cols if any(sub in x for sub in ["_sum_", "_count_"])] +
                    [F.max(F.col(x)).alias(x.replace(old_suffix, new_suffix)) for x in fts_cols if "_max_" in x] +
                    [F.min(F.col(x)).alias(x.replace(old_suffix, new_suffix)) for x in fts_cols if "_min_" in x] 
                )

                # create features for other operators (avg)
                avg_cols = [col for col in lower_fts.columns if "_avg_" in col] # l1w in col name
                for col in avg_cols:
                    aggs.append((F.sum(col.replace("_avg_", "_sum_")) / F.sum(col.replace("_avg_", "_count_"))).alias(f"{col.replace(old_suffix, new_suffix)}"))

                air_fts = lower_fts.groupBy("msisdn").agg(*aggs)
            else: 
                continue
              
        # write to parquet
        utils.save_to_s3(air_fts, f"{out_dir}/{freq_str}/date={run_date}")
        if i==1:
            if level == "weekly":
                df_air_raw.unpersist()

@status_check
def merge_air_fts(spark, out_dir, run_date, **kwargs):
    
    print("merging lxw fts for", run_date)

    l1w = utils.load_from_s3(spark, out_dir + "/l1w").where(f"date='{run_date}'").drop("date")
    l4w = utils.load_from_s3(spark, out_dir + "/l4w").where(f"date='{run_date}'").drop("date")
    l12w = utils.load_from_s3(spark, out_dir + "/l12w").where(f"date='{run_date}'").drop("date")
    l24w = utils.load_from_s3(spark, out_dir + "/l24w").where(f"date='{run_date}'").drop("date")
    
    df_fts = l1w.join(l4w, on="msisdn", how='outer')\
            .join(l12w, on="msisdn", how='outer')\
            .join(l24w, on="msisdn", how='outer')
    
    # agg 
    exclude_list = ["msisdn", "date"]

    # ratio_fts
    for freq_str in ['l1w', 'l4w', 'l12w', 'l24w']:
        df_fts = (
            df_fts.withColumn(f"air_balance_update_count_nt_ratio_{freq_str}", F.expr(f"air_balance_update_count_nt_{freq_str} / air_balance_update_count_{freq_str} "))
                .withColumn(f"air_balance_update_count_wk_ratio_{freq_str}", F.expr(f"air_balance_update_count_wk_{freq_str} / air_balance_update_count_{freq_str} "))
                .withColumn(f"air_balance_update_sum_nt_ratio_{freq_str}", F.expr(f"air_balance_update_sum_nt_{freq_str} / air_balance_update_sum_{freq_str}"))
                .withColumn(f"air_balance_update_sum_wk_ratio_{freq_str}", F.expr(f"air_balance_update_sum_wk_{freq_str} / air_balance_update_sum_{freq_str}"))
                .withColumn(f"air_use_voucher_ratio_{freq_str}", F.expr(f"air_use_voucher_count_{freq_str} / air_balance_update_count_{freq_str}"))
                .withColumn(f"air_adjustment_ratio_{freq_str}", F.expr(f"air_adjustment_count_{freq_str} / air_balance_update_count_{freq_str}"))
                .withColumn(f"air_refill_ratio_{freq_str}", F.expr(f"air_refill_count_{freq_str} / air_balance_update_count_{freq_str}"))
        )

    # Create lxw vs lyw features
    fts_names = [x for x in df_fts.columns if ("l1w" in x)& (x not in exclude_list)&("_min_" not in x)&("_max_" not in x)&("_std_" not in x)&("_avg_" not in x)]
    lxw_list = ['l1w', 'l4w', 'l12w', 'l24w']

    for ft in fts_names:
        for i in range(0, len(lxw_list)):
            for j in range(i+1, len(lxw_list)):
                lxw = lxw_list[i]
                lyw = lxw_list[j]
                new_ft = ft[:ft.rfind('_')] + '_' + lxw + '_vs_' + lyw
                lxw_ft = ft[:ft.rfind('_')] + '_' + lxw
                lyw_ft = ft[:ft.rfind('_')] + '_' + lyw
                df_fts = df_fts.withColumn(new_ft, F.expr(f"{lxw_ft} / {lyw_ft}"))   

     # write to parquet
    utils.save_to_s3(df_fts, f"{out_dir}/final_fts/date={run_date}")

if __name__ == "__main__":
    table_name = "blueinfo_ocs_air"
    run_create_feature(
        func_and_kwargs=(
            (gen_air_lxw_fts, {'level':'weekly'}),
            (gen_air_lxw_fts, {'level':'lxw'}),
            (merge_air_fts, {})
        ),
        global_kwargs={
            'freq': 'weekly',
            'feature_name': 'air',
            'table_name': table_name,

        }
    )