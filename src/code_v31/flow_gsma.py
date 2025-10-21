from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window
from datetime import datetime
from dateutil.relativedelta import relativedelta
import time
import os
import sys
from config_edit import config
import utils

os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'
    
def process_table(spark, table_name, sub_table, ALL_MONTHS, fix_dates):
    first_fix_date_of_month = ALL_MONTHS[0] + fix_dates[0] # random if backtest mode, else fix_dates has only 1 ele 
    
    # date range
    end_date = datetime.strptime(first_fix_date_of_month, "%Y%m%d")   
    start_date = end_date - relativedelta(months=1) + relativedelta(days=1)
    end_date_str = end_date.strftime("%Y%m%d")
    start_date_str = start_date.strftime("%Y%m%d")
    
    if run_mode == 'prod': 
        query = f"select MSISDN, IMEI, s3_file_date from {sub_table_name_on_production} where s3_file_date BETWEEN '{start_date_str}' AND '{end_date_str}' and IMEI <> ''"
    else: 
        query = f"select MSISDN, IMEI, s3_file_date from {sub_table_name_on_production} where s3_file_date BETWEEN '{start_date_str}' AND '{end_date_str}' and IMEI <> '' and msisdn in (select {backtest_table_phone_name} from {backtest_table_name})"
    
    n_partitions = spark.sparkContext.defaultParallelism * 4
    usage = utils.spark_read_data_from_singlestore(spark, query)
    usage = usage.repartition(n_partitions)

    usage = usage.withColumn("latest_date", F.max("s3_file_date").over(Window.partitionBy("MSISDN"))).filter(F.col("s3_file_date") == F.col("latest_date"))

    ## union data for backtest mode
    if run_mode == 'backtest': ## add
        usage = usage.withColumn("SNAPSHOT", F.lit(end_date))
        for MONTH in ALL_MONTHS:    
            for fix_date in fix_dates:
                fix_date_of_month = MONTH + fix_date
                if fix_date_of_month == first_fix_date_of_month:
                    continue
                print(f"Process {fix_date_of_month}")

                # date range
                end_date = datetime.strptime(fix_date_of_month, "%Y%m%d")   
                start_date = end_date - relativedelta(months=1) + relativedelta(days=1)
                end_date_str = end_date.strftime("%Y%m%d")
                start_date_str = start_date.strftime("%Y%m%d")
                query = f"select MSISDN, IMEI, s3_file_date from {sub_table_name_on_production} where s3_file_date BETWEEN '{start_date_str}' AND '{end_date_str}' and IMEI <> '' and msisdn in (select {backtest_table_phone_name} from {backtest_table_name})"
                usage2 = utils.spark_read_data_from_singlestore(spark, query)
                usage2 = usage2.withColumn("latest_date", F.max("s3_file_date").over(Window.partitionBy("MSISDN"))).filter(F.col("s3_file_date") == F.col("latest_date"))
                usage2 = usage2.withColumn("SNAPSHOT", F.lit(end_date))

                ### union
                if usage.columns == usage2.columns:
                    usage = usage.union(usage2)
                    print(' - Union ...')
        ## drop duplicates 
        usage = usage.dropDuplicates(subset = ['MSISDN', 's3_file_date','SNAPSHOT'])
    else: 
        usage = usage.dropDuplicates(subset = ['MSISDN', 's3_file_date'])

    # GSMA
    sub_query = f"select max(s3_file_date) max_date from {table_name_on_production}"
    max_date = utils.spark_read_data_from_singlestore(spark, sub_query).toPandas()['max_date'].values[0]
    query = f"select * from {table_name_on_production} where s3_file_date = '{max_date}'"
    tac_gsma = utils.spark_read_data_from_singlestore(spark, query)
    tac_gsma = tac_gsma.drop_duplicates(subset = ['TAC']) 
    tac_gsma = tac_gsma.select(
        "TAC",
        "GSMA_Operating_System",
        "Standardised_Full_Name",
        "Standardised_Device_Vendor",
        "Standardised_Marketing_Name",
        "Year_Released",
        "Internal_Storage_Capacity",
        "Expandable_Storage",
        "Total_Ram"
    )

    # Join
    usage = usage.withColumn('key', F.substring(F.col("IMEI"), 1, 8))
    usage = usage.join(tac_gsma, on = (usage['key'] == tac_gsma['tac']), how = 'left')
    
    # Rename
    usage = usage.withColumnRenamed("MSISDN", "msisdn")
    usage = usage.drop("latest_date")    

    # Add year release delta
    usage =  usage.withColumn('Year_released_delta', F.year(F.current_date()) - F.col('Year_Released').cast(T.IntegerType()) )

    # Fill NA
    fill_na_stratery = {
        'GSMA_Operating_System': 'null',
        'Standardised_Full_Name': 'null',
        'Standardised_Device_Vendor': 'null',
        'Standardised_Marketing_Name': 'null',
        'Year_Released': '0',
        'Internal_Storage_Capacity': 0,
        'Expandable_Storage': '0',
        'Total_Ram': '0',
        
    }
    usage_filled = usage.fillna(fill_na_stratery)

    ### drop
    usage_filled = usage_filled.drop('IMEI', 's3_file_date', 'key')

    # Save
    if run_mode == 'prod':
        target_file_name = f"merged/date={first_fix_date_of_month}"
    else: 
        target_file_name = "merged/"
    usage_filled = usage_filled.dropDuplicates()
    utils.save_to_s3(usage_filled, table_name, target_file_name, run_mode)

table_name = 'blueinfo_tac_gsma'
table_name_on_production = config.table_dict[table_name]
sub_table = 'blueinfo_ocs_crs_usage'
sub_table_name_on_production = config.table_dict[sub_table]
sub_table_name_on_backest = ""

ALL_MONTHS = config.ALL_MONTHS
ALL_MONTHS.sort(reverse = True)
fix_dates = config.fix_date
run_mode = config.run_mode
backtest_table_name = config.backtest_table_dict[table_name]['backtest_table_name']
backtest_table_phone_name = config.backtest_table_dict[table_name]['backtest_table_phone_name']

# create Spark session
spark = utils.create_spark_instance(run_mode)

# Execute run_all_month function
if run_mode == 'prod':
    is_table_finished = utils.check_table_finished(spark, table_name,  ALL_MONTHS, fix_dates[0], run_mode) 
    if is_table_finished:
        print("Skip")
    else:
        process_table(spark, table_name, sub_table, ALL_MONTHS, fix_dates)
else:
    process_table(spark, table_name, sub_table, ALL_MONTHS, fix_dates)