import sys
from pyspark.sql import functions as F, types as T
from datetime import datetime
from dateutil.relativedelta import relativedelta, MO
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from src.utils import common as utils 
from src.utils.job_quality_utils import status_check, run_create_feature

fee_cate_str = """
    CASE WHEN KHOANMUCTT_ID IN ('1', '200', '201', '202', '203', '204', '205', '206') THEN 'basic_charge'
         WHEN KHOANMUCTT_ID IN ('400', '401', '600', '601', '602', '603') THEN 'plans_promotions'
         WHEN KHOANMUCTT_ID IN ('701', '702', '703', '704', '705', '706', '707', '711', '712', '713') THEN 'contract_services'
         ELSE 'other'
    END
"""

@status_check
def gen_tra_l1m_fts(spark, table_name, bt_table_name, bt_msisdn_column, out_dir, run_date, run_mode, common_config):
    print("generating lxw fts for:", run_date)
    date_to = datetime.strptime(run_date, "%Y%m%d")

    # if day > 15 then month -2 else month -3
    if date_to.day >= 15:
        latest_month = (date_to - relativedelta(months=2)).strftime("%Y%m")
    else:
        latest_month = (date_to - relativedelta(months=3)).strftime("%Y%m")
        
    freq_str = f"l1m"

    query = f"""
        SELECT MA_TB as msisdn, KHOANMUCTT_ID, TRAGOC, TRATHUE, s3_file_date
        FROM {table_name}
        WHERE 1=1
            AND s3_file_date = '{latest_month}'
    """
        
    df_raw = utils.spark_read_data_from_singlestore(spark, query).persist()
    df_raw.count()

    if run_mode == 'prod': 
        df_raw = df_raw.where("left(msisdn, 4) = '0084' and length(msisdn) = 13")
     
    df_fts = df_raw.withColumn("fee_cate", F.expr(fee_cate_str))\
                    .withColumn("amount", F.expr("TRAGOC + TRATHUE"))
    # create features

    aggs = []
    aggs.append(F.countDistinct("KHOANMUCTT_ID").alias(f"tra_total_items_count_distinct_{freq_str}"))
    aggs.append(F.sum("TRAGOC").alias(f"tra_total_debt_sum_{freq_str}"))
    aggs.append(F.sum("TRATHUE").alias(f"tra_total_tax_sum_{freq_str}"))
    aggs.append(F.sum("amount").alias(f"tra_total_amount_sum_{freq_str}"))

    groups = ['basic_charge', 'plans_promotions', 'contract_services', 'other']
    for group in groups:
        aggs.extend(
            [F.countDistinct(F.expr(f"CASE WHEN fee_cate = '{group}' THEN KHOANMUCTT_ID END")).alias(f"tra_{group}_items_count_distinct_{freq_str}")] +
            [F.sum(F.expr(f"CASE WHEN fee_cate = '{group}' THEN TRAGOC END")).alias(f"tra_{group}_debt_sum_{freq_str}")] +
            [F.max(F.expr(f"CASE WHEN fee_cate = '{group}' THEN TRAGOC END")).alias(f"tra_{group}_debt_max_{freq_str}")] +
            [F.stddev(F.expr(f"CASE WHEN fee_cate = '{group}' THEN TRAGOC END")).alias(f"tra_{group}_debt_std_{freq_str}")] +
            
            [F.sum(F.expr(f"CASE WHEN fee_cate = '{group}' THEN TRATHUE END")).alias(f"tra_{group}_tax_sum_{freq_str}")] +
            [F.max(F.expr(f"CASE WHEN fee_cate = '{group}' THEN TRATHUE END")).alias(f"tra_{group}_tax_max_{freq_str}")] +
            [F.stddev(F.expr(f"CASE WHEN fee_cate = '{group}' THEN TRATHUE END")).alias(f"tra_{group}_tax_std_{freq_str}")] +
            
            [F.sum(F.expr(f"CASE WHEN fee_cate = '{group}' THEN amount END")).alias(f"tra_{group}_amount_sum_{freq_str}")] +
            [F.max(F.expr(f"CASE WHEN fee_cate = '{group}' THEN amount END")).alias(f"tra_{group}_amount_max_{freq_str}")] +
            [F.stddev(F.expr(f"CASE WHEN fee_cate = '{group}' THEN amount END")).alias(f"tra_{group}_amount_std_{freq_str}")]
        )
    
    df_fts = df_fts.groupBy("msisdn").agg(*aggs)
    
    # ratio features
    for group in groups:
        debt_ratio = f"tra_{group}_debt_sum_{freq_str} / tra_total_debt_sum_{freq_str}"
        amount_ratio = f"tra_{group}_amount_sum_{freq_str} / tra_total_amount_sum_{freq_str}"

        df_fts = df_fts.withColumn(f"tra_group_debt_ratio_{freq_str}", F.expr(debt_ratio))\
                        .withColumn(f"tra_group_amount_ratio_{freq_str}", F.expr(amount_ratio))        
            
        # write to parquet
    utils.save_to_s3(df_fts, f"{out_dir}/final_fts/date={run_date}")

    df_raw.unpersist()

if __name__ == "__main__":
    table_name = "blueinfo_ccbs_ct_tra"

    run_create_feature(
        func_and_kwargs=(
            (gen_tra_l1m_fts, {}),
        ),
        global_kwargs={
            'freq': 'weekly',
            'feature_name': 'tra',
            'table_name': table_name,

        }
    )