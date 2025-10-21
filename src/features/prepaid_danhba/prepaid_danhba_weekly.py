from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql import Window
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
import time
import sys
from src.utils import common as utils 
from src.utils.job_quality_utils import status_check, run_create_feature

import os
os.environ["LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN"] = "1"


gender_str = """
    CASE WHEN gender = 'Nữ' or gender = 'female' or gender = 'NỮ / F' THEN 0
         WHEN gender = 'Nam' or gender = 'male' or gender = 'NAM / M' THEN 1
    ELSE 2 END
"""

phai_str = """
    CASE WHEN phai = 0 THEN 0
         WHEN phai = 1 THEN 1
    ELSE 2 END
"""

modifydate_str = """
    CASE 
        WHEN to_timestamp(modifydate, 'dd/MM/yyyy HH:mm:ss') IS NOT NULL 
        THEN modifydate 
        ELSE '' 
    END
"""

ngay_cn_str = """
    CASE
        WHEN to_timestamp(ngay_cn, 'dd/MM/yyyy HH:mm:ss') IS NOT NULL
        THEN ngay_cn
        ELSE ''
    END
"""
sex_str = """
    CASE 
        WHEN modifydate >= ngay_cn THEN gender
        ELSE phai
    END
"""

age_group_str = """
    CASE
        WHEN modifydate >= ngay_cn THEN age
        ELSE nhom_tuoi
    END
"""

@status_check
def gen_prepaid_danhba(
        spark, prepaid_table_name, danhba_table_name,
        prepaid_dev_name, danhba_dev_name,
        bt_table_name, bt_msisdn_column, out_dir,
        run_date, run_mode, common_config
    ):
    """
        This table has the difference database on production
        --> using dev name to detect prod database    
    """
    freq_str = "l1w"
    
    print("generating weekly fts for:", run_date)
    
    ### load prepaid
    query = f"""
        SELECT 
            t.msisdn,
            t.gender,
            t.age,
            t.registerdate,
            t.modifydate,
            t.s3_file_date
        FROM 
            {prepaid_table_name} t
        join (select msisdn, max(s3_file_date) as lasted_date
            from {prepaid_table_name}
            where modifydate is not null 
                and s3_file_date < '{run_date}'
            group by msisdn
            ) lastest
        on t.msisdn = lastest.msisdn and t.s3_file_date = lastest.lasted_date
    """
    prepaid = utils.spark_read_data_from_singlestore(spark, query, prepaid_dev_name).persist()
    prepaid.count() 
    if run_mode == 'prod': 
        prepaid = prepaid.where("left(msisdn, 4) = '0084' and length(msisdn) = 13")
    
    window1 = Window.partitionBy("msisdn").orderBy(F.desc(F.col("modifydate")))
    df_prepaid = (
        prepaid.withColumn('modifydate', F.expr(modifydate_str))
            .fillna('01/01/1980 00:00:00', subset = ['modifydate'])
            .withColumn("modifydate", F.to_timestamp(F.col("modifydate"), 'dd/MM/yyyy HH:mm:ss'))
            .withColumn('gender', F.expr(gender_str))
            .withColumn('rank', F.row_number().over(window1))
            .where(F.col('rank') == 1)
            .drop('rank')
    )

    ### process danhba
    query = f"""
        SELECT
            t.somay,
            t.nhom_tuoi,
            t.phai,
            t.ngay_ld,
            t.ngay_cn,
            t.s3_file_date
        FROM {danhba_table_name} t
        join (select somay, max(s3_file_date) as lasted_date
            from {danhba_table_name}
            where ngay_cn IS NOT NULL
                and s3_file_date < '{run_date}'
            group by somay
            )lastest
        on t.somay = lastest.somay and t.s3_file_date = lastest.lasted_date
    """
    danhba = utils.spark_read_data_from_singlestore(spark, query, danhba_dev_name).persist()
    danhba.count()
    if run_mode == 'prod': 
        danhba = danhba.where("left(somay, 4) = '0084' and length(somay) = 13")

    window2 = Window.partitionBy("somay").orderBy(F.desc(F.col("ngay_cn")))
    df_danhba = (
        danhba.withColumn('ngay_cn', F.expr(ngay_cn_str))
            .fillna('01/01/1980 00:00:00', subset = ['ngay_cn'])
            .withColumn('ngay_cn', F.to_timestamp(F.col("ngay_cn"), 'dd/MM/yyyy HH:mm:ss'))
            .withColumn('phai', F.expr(phai_str))
            .withColumn('rank_2', F.row_number().over(window2))
            .where(F.col('rank_2') == 1)
            .withColumnRenamed("somay", "msisdn")
            .drop("rank_2")
    )
    
    ### join 2 tables
    df_fts = (
        df_prepaid.join(df_danhba, 'msisdn', how ="fullouter")
            .withColumn("modifydate", F.col("modifydate").cast(T.StringType()))
            .withColumn('ngay_cn', F.col("ngay_cn").cast(T.StringType()))
            .fillna('1980-01-01 00:00:00', subset = ['modifydate', 'ngay_cn'])
            .withColumn("modifydate", F.to_timestamp(F.col("modifydate"), 'yyyy-MM-dd HH:mm:ss'))
            .withColumn('ngay_cn', F.to_timestamp(F.col("ngay_cn"), 'yyyy-MM-dd HH:mm:ss'))
            .withColumn(f'prepaid_danhba_sex_{freq_str}', F.expr(sex_str))
            .withColumn(f'prepaid_danhba_age_group_{freq_str}', F.expr(age_group_str))
            .select('msisdn', f'prepaid_danhba_sex_{freq_str}', f'prepaid_danhba_age_group_{freq_str}')
            .dropDuplicates()
    )
    
    # save to parquet
    
    utils.save_to_s3(df_fts, out_dir + f"/{freq_str}/date={run_date}")
    danhba.unpersist()
    prepaid.unpersist()


if __name__ == "__main__":
    common_config = utils.load_config("configs", "common.yaml")
    to_edit_config = utils.load_config("config_edit", "to_edit.yaml")

    # directory and table
    prepaid_table_name = "blueinfo_dbvnp_prepaid_subscribers_history"
    prepaid_production = to_edit_config['table_name'][prepaid_table_name]

    danhba_table_name = "blueinfo_ccbs_ccs_xxx_danhba_dds_pttb"
    danhba_production = to_edit_config['table_name'][danhba_table_name]

    bt_table_name = to_edit_config['backtest_table_dict'][prepaid_table_name]['backtest_table_name']
    bt_msisdn_column = to_edit_config['backtest_table_dict'][prepaid_table_name]['msisdn_column']

    run_create_feature(
        func_and_kwargs=(
            (gen_prepaid_danhba, {
                'prepaid_table_name': prepaid_production, 'danhba_table_name':danhba_production,
                'prepaid_dev_name': prepaid_table_name, 'danhba_dev_name': danhba_table_name,
                'bt_table_name': bt_table_name, 'bt_msisdn_column': bt_msisdn_column,
                }),
        ),
        global_kwargs={
            'freq': 'weekly',
            'feature_name': 'prepaid_danhba',
            'table_name': prepaid_table_name,

        }
    )

