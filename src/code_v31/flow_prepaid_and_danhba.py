from pyspark.sql import Window, functions as F, types as T
import sys
import os
from config_edit import config
import utils as utils

os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

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

# prepaid table
def process_subscribers_history(spark, table_name, table_name_on_production, fix_date_of_month):
    window1 = Window.partitionBy("msisdn").orderBy(F.desc(F.col("modifydate")))

    query = f"""
        SELECT 
            t.msisdn,
            t.gender,
            t.age,
            t.registerdate,
            t.modifydate,
            t.s3_file_date
        FROM 
            {table_name_on_production} t
        join (select msisdn, max(s3_file_date) as lasted_date
            from {table_name_on_production}
            where modifydate is not null 
                and s3_file_date <= {fix_date_of_month}
            group by msisdn
            ) lastest
        on t.msisdn = lastest.msisdn and t.s3_file_date = lastest.lasted_date
    """
    if run_mode == 'backtest': 
        query += f"where t.msisdn in (select {backtest_table_phone_name} from {backtest_table_name})"

    prepaid = (
        utils.spark_read_data_from_singlestore(spark, query, table_name)
             .withColumn('modifydate', F.expr(modifydate_str))
             .fillna('01/01/1980 00:00:00', subset = ['modifydate'])
             .withColumn("modifydate", F.to_timestamp(F.col("modifydate"), 'dd/MM/yyyy HH:mm:ss'))
             .withColumn('gender', F.expr(gender_str))
             .withColumn('rank', F.row_number().over(window1))
             .where(F.col('rank') == 1)
             .drop('rank')
    )
    if run_mode == 'prod':
        target_file_name = f"merged/date={fix_date_of_month}"
    else: 
        target_file_name = f"merged"

    utils.save_to_s3(prepaid, table_name, target_file_name, run_mode)

# danhba table
def process_danhba(spark, table_name, table_name_on_production, fix_date_of_month):
    window2 = Window.partitionBy("somay").orderBy(F.desc(F.col("ngay_cn")))

    query = f"""
        SELECT
            t.somay,
            t.nhom_tuoi,
            t.phai,
            t.ngay_ld,
            t.ngay_cn,
            t.s3_file_date
        FROM {table_name_on_production} t
        join (select somay, max(s3_file_date) as lasted_date
            from {table_name_on_production}
            where ngay_cn IS NOT NULL
                and s3_file_date <= {fix_date_of_month}
            group by somay
            )lastest
        on t.somay = lastest.somay and t.s3_file_date = lastest.lasted_date
    """
    if run_mode == 'backtest': 
        query += f"where t.somay in (select {backtest_table_phone_name} from {backtest_table_name})"
    
    danhba = (
        utils.spark_read_data_from_singlestore(spark, query, table_name)
             .withColumn('ngay_cn', F.expr(ngay_cn_str))
             .fillna('01/01/1980 00:00:00', subset = ['ngay_cn'])
             .withColumn('ngay_cn', F.to_timestamp(F.col("ngay_cn"), 'dd/MM/yyyy HH:mm:ss'))
             .withColumn('phai', F.expr(phai_str))
             .withColumn('rank_2', F.row_number().over(window2))
             .where(F.col('rank_2') == 1)
    )
    if run_mode == 'prod':
        target_file_name = f"merged/date={fix_date_of_month}"
    else: 
        target_file_name = f"merged"
    utils.save_to_s3(danhba, table_name, target_file_name, run_mode)

# Merge 2 tables
def merge_2_table(spark, table_name, subscriber_hist_table, danhba_table, fix_date_of_month):

    if run_mode == 'prod':
        target_file_name = f"merged/date={fix_date_of_month}"
    else: 
        target_file_name = f"merged"

    #prepaid table
    pre_process_prepaid_loaded = utils.load_from_s3(spark, subscriber_hist_table , target_file_name, run_mode)
    
    #subscriber table
    pre_process_danhba = utils.load_from_s3(spark, danhba_table , target_file_name, run_mode)\
        .withColumnRenamed("somay", "msisdn")
    
    # join 
    joined_2_table = (
        pre_process_prepaid_loaded.join(pre_process_danhba, 'msisdn', how ="fullouter")
            .withColumn("modifydate", F.col("modifydate").cast(T.StringType()))
            .withColumn('ngay_cn', F.col("ngay_cn").cast(T.StringType()))
            .fillna('1980-01-01 00:00:00', subset = ['modifydate', 'ngay_cn'])
            .withColumn("modifydate", F.to_timestamp(F.col("modifydate"), 'yyyy-MM-dd HH:mm:ss'))
            .withColumn('ngay_cn', F.to_timestamp(F.col("ngay_cn"), 'yyyy-MM-dd HH:mm:ss'))
            .withColumn('Sex', F.expr(sex_str))
            .withColumn('Age_group', F.expr(age_group_str))
            .fillna({'Sex': 2 ,'Age_group': 'ukn'})
            .select('msisdn', 'Sex', 'Age_group')
            .dropDuplicates()
    )
    if run_mode == 'prod':
        target_file_name = f"merged/date={fix_date_of_month}"
    else: 
        target_file_name = f"merged"
    utils.save_to_s3(joined_2_table, table_name, target_file_name, run_mode)
    print(f'FINISH {table_name}')

if __name__ == '__main__':
    ALL_MONTHS = config.ALL_MONTHS
    ALL_MONTHS.sort(reverse = True)
    fix_dates = config.fix_date

    fix_date_of_all_months = []
    for fix_date in fix_dates:

        fix_date_of_all_months += list(map(lambda x: x + fix_date, ALL_MONTHS))

    fix_date_of_all_months.sort(reverse=True)
    fix_date_of_month = fix_date_of_all_months[0]
    run_mode = config.run_mode

    file_name =  'prepaid_and_danhba'
    backtest_table_name = config.backtest_table_dict[file_name]['backtest_table_name']
    backtest_table_phone_name = config.backtest_table_dict[file_name]['backtest_table_phone_name']

    spark = utils.create_spark_instance(run_mode)

    # process subscriber history
    subscriber_hist_table = 'blueinfo_dbvnp_prepaid_subscribers_history'
    subscriber_hist_table_on_production = config.table_dict[subscriber_hist_table]
    process_subscribers_history(spark, subscriber_hist_table, subscriber_hist_table_on_production, fix_date_of_month)
    
    # process danhba
    danhba_table = 'blueinfo_ccbs_ccs_xxx_danhba_dds_pttb'
    danhba_table_on_production = config.table_dict[danhba_table]
    process_danhba(spark, danhba_table, danhba_table_on_production, fix_date_of_month)
    
    # merge 2 table
    table_name = 'prepaid_and_danhba'
    table_name_on_production = config.table_dict[table_name]
    merge_2_table(spark, table_name, subscriber_hist_table, danhba_table, fix_date_of_month)

