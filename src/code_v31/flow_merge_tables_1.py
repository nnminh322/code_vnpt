from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, DoubleType, IntegerType
# import xgboost as xgb   # version 2.1.1
import os
import sys
from config_edit import config
import utils

os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'


ALL_MONTHS = config.ALL_MONTHS
ALL_MONTHS.sort(reverse = True)
fix_dates = config.fix_date
fix_date = fix_dates[0] 
run_mode = config.run_mode

spark = utils.create_spark_instance(run_mode)

# Full table list
full_table_list = [
    'blueinfo_ccbs_ct_no',
    'blueinfo_ccbs_ct_tra',
    'blueinfo_ccbs_cv207',
    'blueinfo_ccbs_spi_3g_subs',
    'blueinfo_ggsn',
    'blueinfo_ocs_air',
    'blueinfo_ocs_crs_usage',
    'blueinfo_vascdr_2friend_log',
    'blueinfo_vascdr_brandname_meta',
    'blueinfo_vascdr_udv_credit_log',
    'blueinfo_vascdr_utn_credit_log',
    'blueinfo_vascloud_da',
    'blueinfo_voice_msc',
    'blueinfo_voice_volte',
    'blueinfo_tac_gsma',
    'prepaid_and_danhba',
    'blueinfo_smrs_dwd_geo_rvn_mtd_part1',
    'blueinfo_smrs_dwd_geo_rvn_mtd_part2',
    'blueinfo_smrs_dwd_geo_rvn_mtd_part3',
    'blueinfo_smrs_dwd_geo_rvn_mtd_part4',
]
full_table_list = list(map(lambda x: x , full_table_list))

# Table list
table_list = [
    'blueinfo_ccbs_ct_no',
    'blueinfo_ccbs_ct_tra',
    'blueinfo_ccbs_cv207',
    'blueinfo_ccbs_spi_3g_subs',
    'blueinfo_ggsn',
]
table_list = list(map(lambda x: x ,  table_list ))

# Find merged version
full_tables = {}
for table_name_with_version in full_table_list:
    merged_file = f"merged/date={ALL_MONTHS[0] + fix_date}"
    #### check merged_file exist
    try:
        utils.load_from_s3(spark, table_name_with_version, merged_file)
    except:
        print(f'{table_name_with_version}/{merged_file} not exits')
        raise Exception(f'{table_name_with_version}/{merged_file} not exist')
        break
    full_tables[table_name_with_version] = merged_file
    print(f"{table_name_with_version} : {merged_file}")

tables = {}
for table_name_with_version in table_list:
    merged_file = f"merged/date={ALL_MONTHS[0] + fix_date}"
    #### check merged_file exist
    try:
        utils.load_from_s3(spark, table_name_with_version, merged_file)
    except:
        print(f'{table_name_with_version}/{merged_file} not exits')
        raise Exception(f'{table_name_with_version}/{merged_file} not exits')
        break
    tables[table_name_with_version] = merged_file
    print(f"{table_name_with_version} : {merged_file}")

# Find unique msisdn from cross all tables
df_first = utils.load_from_s3(spark, list(full_tables.keys())[0], list(full_tables.values())[0])
print(f'Table: {list(full_tables.keys())[0]}')
msisdn_list = df_first.select('msisdn')
print(f'msisdn count: {msisdn_list.count()}')
for table_name in list(full_tables.keys())[1:]:
    print(f'Table: {table_name}')
    df_2 = utils.load_from_s3(spark, table_name, full_tables[table_name])
    msisdn_list_2 = df_2.select('msisdn')
    print(f'msisdn count: {msisdn_list_2.count()}')
    msisdn_list = msisdn_list.union(msisdn_list_2)
msisdn_unique_list = msisdn_list.drop_duplicates()
print(f'msisdn unique count: {msisdn_unique_list.count()}')

# load and fill na
def fill_na_numeric(df):
    df = df.fillna(0.0)
    return df
  
def fill_na_category(df):
    columns = df.columns
    item_columns = list(filter(lambda x: '-item' in x, columns ))
    other_columns = list(filter(lambda x: '-item' not in x, columns ))
    key_list = item_columns + other_columns
    value_list = ['null'] * len(item_columns) + [0.0] * len(other_columns)
    filled_dict = dict(zip(key_list, value_list))
    df = df.fillna(filled_dict)
    return df

def fill_na_tac_gsma(df):
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
    df = df.fillna(fill_na_stratery)
    return df

def fill_na_prepaid_and_danhba(df):
    df = df.fillna({'Sex': 2 ,'Age_group': 'ukn'})
    return df

def fill_na_device(df): 
    int_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() in ("int", "bigint")]
    float_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() == "double"]
    string_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() == "string"]

    df_filled = df.fillna({**{col: 0 for col in int_cols},
                        **{col: 0.0 for col in float_cols},
                        **{col: "ukn" for col in string_cols}})
    
    return df_filled

for table_name in list(tables.keys()):
    print(f'Table: {table_name}')
    df_load = utils.load_from_s3(spark, table_name, tables[table_name])
    df_load = df_load.filter('msisdn is not null')
    df_merge = (
        df_load.withColumnRenamed("msisdn", "msisdn_df1")
        .join(msisdn_unique_list.withColumnRenamed("msisdn", "msisdn_df2"), F.col('msisdn_df1') == F.col('msisdn_df2'), how ="fullouter")
        .withColumn('msisdn_new', F.coalesce("msisdn_df1", "msisdn_df2"))
        .drop('msisdn_df1', 'msisdn_df2')
        .withColumnRenamed("msisdn_new", "msisdn")
        .filter('msisdn is not null')
    )
        
    # Fill N/A
    if table_name.startswith('blueinfo_ccbs_ct_no'):
        df_filled = fill_na_numeric(df_merge)
        print(f"Save to {tables[table_name] + '_filled'} ...")
        utils.save_to_s3(df_filled, table_name, tables[table_name] + '_filled')
    
    elif table_name.startswith('blueinfo_ccbs_ct_tra'):
        df_filled = fill_na_numeric(df_merge)
        print(f"Save to {tables[table_name] + '_filled'} ...")
        utils.save_to_s3(df_filled, table_name, tables[table_name] + '_filled')
    
    elif table_name.startswith('blueinfo_ccbs_cv207'):
        df_filled = fill_na_category(df_merge)
        print(f"Save to {tables[table_name] + '_filled'} ...")
        utils.save_to_s3(df_filled, table_name, tables[table_name] + '_filled')
        
    elif table_name.startswith('blueinfo_ccbs_spi_3g_subs'):
        df_filled = fill_na_category(df_merge)
        print(f"Save to {tables[table_name] + '_filled'} ...")
        utils.save_to_s3(df_filled, table_name, tables[table_name] + '_filled')

    elif table_name.startswith('blueinfo_ggsn'):
        df_filled = fill_na_numeric(df_merge)
        print(f"Save to {tables[table_name] + '_filled'} ...")
        utils.save_to_s3(df_filled, table_name, tables[table_name] + '_filled')
        
    elif table_name.startswith('blueinfo_ocs_air'):
        df_filled = fill_na_numeric(df_merge)
        print(f"Save to {tables[table_name] + '_filled'} ...")
        utils.save_to_s3(df_filled, table_name, tables[table_name] + '_filled')
        
    elif table_name.startswith('blueinfo_ocs_crs_usage'):
        df_filled = fill_na_numeric(df_merge)
        print(f"Save to {tables[table_name] + '_filled'} ...")
        utils.save_to_s3(df_filled, table_name, tables[table_name] + '_filled')
        
    elif table_name.startswith('blueinfo_vascdr_2friend_log'):
        df_filled = fill_na_numeric(df_merge)
        print(f"Save to {tables[table_name] + '_filled'} ...")
        utils.save_to_s3(df_filled, table_name, tables[table_name] + '_filled')
        
    elif table_name.startswith('blueinfo_vascdr_brandname_meta'):
        df_filled = fill_na_category(df_merge)
        print(f"Save to {tables[table_name] + '_filled'} ...")
        utils.save_to_s3(df_filled, table_name, tables[table_name] + '_filled')
        
    elif table_name.startswith('blueinfo_vascdr_udv_credit_log'):
        df_filled = fill_na_numeric(df_merge)
        print(f"Save to {tables[table_name] + '_filled'} ...")
        utils.save_to_s3(df_filled, table_name, tables[table_name] + '_filled')
        
    elif table_name.startswith('blueinfo_vascdr_utn_credit_log'):        
        df_filled = fill_na_numeric(df_merge)
        print(f"Save to {tables[table_name] + '_filled'} ...")
        utils.save_to_s3(df_filled, table_name, tables[table_name] + '_filled')
        
    elif table_name.startswith('blueinfo_vascloud_da'):
        df_filled = fill_na_numeric(df_merge)
        print(f"Save to {tables[table_name] + 'filled'} ...")
        utils.save_to_s3(df_filled, table_name, tables[table_name] + '_filled')
        
    elif table_name.startswith('blueinfo_voice_msc'):
        df_filled = fill_na_numeric(df_merge)
        print(f"Save to {tables[table_name] + '_filled'} ...")
        utils.save_to_s3(df_filled, table_name, tables[table_name] + '_filled')
        
    elif table_name.startswith('blueinfo_voice_volte'):
        df_filled = fill_na_numeric(df_merge)
        utils.save_to_s3(df_filled, table_name, tables[table_name] + '_filled')
        
    elif table_name.startswith('blueinfo_tac_gsma'):
        df_filled = fill_na_tac_gsma(df_merge)
        print(f"Save to {tables[table_name] + '_filled'} ...")
        utils.save_to_s3(df_filled, table_name, tables[table_name] + '_filled')
        
    elif table_name.startswith('prepaid_and_danhba'):
        df_filled = fill_na_prepaid_and_danhba(df_merge)
        print(f"Save to {tables[table_name] + '_filled'} ...")
        utils.save_to_s3(df_filled, table_name, tables[table_name] + '_filled')
        
    elif table_name.startswith('blueinfo_smrs_dwd_geo_rvn_mtd'):
        df_filled = fill_na_numeric(df_merge)
        print(f"Save to {tables[table_name] + '_filled'} ...")
        utils.save_to_s3(df_filled, table_name, tables[table_name] + '_filled')

    elif table_name.startswith('sub'):
        
        df_filled = fill_na_numeric(df_merge)
        
        print(f"Save to {tables[table_name] + '_filled'} ...")
        utils.save_to_s3(df_filled, table_name, tables[table_name] + '_filled')
    
    elif table_name.startswith('device'):
        
        df_filled = fill_na_device(df_merge)
        
        print(f"Save to {tables[table_name] + '_filled'} ...")
        utils.save_to_s3(df_filled, table_name, tables[table_name] + '_filled')

# Merged all tables
df_first = utils.load_from_s3(spark, list(tables.keys())[0], list(tables.values())[0] + '_filled')
df_first = df_first.filter("msisdn is not null")
print(f'Table: {list(tables.keys())[0]}')
for table_name in list(tables.keys())[1:]:
    print(f'Table: {table_name}')
    df_2 = utils.load_from_s3(spark, table_name, tables[table_name] + '_filled')
    df_2 = df_2.filter("msisdn is not null")
    if df_first.count() == df_2.count() :  
        # join
        df_merge = (
            df_first.withColumnRenamed("msisdn", "msisdn_df1")
            .join(
                df_2.withColumnRenamed("msisdn", "msisdn_df2"), 
                F.col('msisdn_df1') == F.col('msisdn_df2'), 
                how ="inner"
            )
                 .drop('msisdn_df2', 'SNAPSHOT_df2', 'LABEL_df2')
                 .withColumnRenamed("msisdn_df1", "msisdn")
                 .withColumnRenamed("SNAPSHOT_df1", "SNAPSHOT")
                 .withColumnRenamed("LABEL_df1", "LABEL")
        )

        # check 
        if (len(df_merge.columns) == len(df_first.columns) + len(df_2.columns) - 1) and (df_merge.count() == df_first.count()):
            print('  - Merged successfully')
            print(f'  - Features count accumulated: {len(df_merge.columns)}')
            df_first = df_merge
        else:
            print('  - Merged falsed ...........')
            raise Exception("RUN FAILED")
            break
    else:
        print(f'Check table {table_name} again ......')
        raise Exception("RUN FAILED")

# fill na
df_merge = df_merge.fillna(0)

# Save
# target_file_name = f"dataset_{ALL_MONTHS[0]}-{ALL_MONTHS[-1]}"
part_num = 'part1'
target_file_name = f"dataset_{ALL_MONTHS[0] + fix_date}-{ALL_MONTHS[-1] + fix_date}_{part_num}"
utils.save_dataset(df_merge, target_file_name)