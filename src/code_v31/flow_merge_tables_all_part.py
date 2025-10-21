from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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


#### RUN
part_list = []

for part in ['part1', 'part2', 'part3', 'part4']:

    target_file_name = f"dataset_{ALL_MONTHS[0] + fix_date}-{ALL_MONTHS[-1] + fix_date}_{part}"

    part_list.append(target_file_name)

part_list.sort()



df_first = utils.load_dataset(spark, part_list[0])


print(f'Dataset: {part_list[0]}')
    

for part in part_list[1:]:


    print(f'Dataset: {part}')


    df_2 = utils.load_dataset(spark, part)
    

    if df_first.count() == df_2.count() :  

        # join
        df_merge = (
            df_first.withColumnRenamed("msisdn", "msisdn_df1")
            .join(
                df_2.withColumnRenamed("msisdn", "msisdn_df2"), 
                F.col('msisdn_df1') == F.col('msisdn_df2'), 
                how ="inner"
            )
                 .drop('msisdn_df2')
                 .withColumnRenamed("msisdn_df1", "msisdn")
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
        
        raise Exception("RUN FAILED")



### Merged with Device and Sub

sub = utils.load_from_s3(spark, 'sub' , f"final_fts/date={ALL_MONTHS[0] + fix_date}")

device = utils.load_from_s3(spark, 'device' , f"date={ALL_MONTHS[0] + fix_date}")

print(f'Merged sub + device')


# merge
df_merge_2 = df_merge.join(sub, ["msisdn"], how='left').join(device, ["TAC"], how='left')


# check 

if (len(df_merge_2.columns) == len(df_merge.columns) + len(sub.columns) + len(device.columns) - 2) and (df_merge.count() == df_merge_2.count()) :

    print('  - Merged successfully')

    print(f'  - Features count accumulated: {len(df_merge.columns)}')


else:

    print('  - Merged falsed ...........')

    raise Exception("  - Merged falsed ...........")



### fill na

df_merge_2 = df_merge_2.fillna(0)



#### Save

# target_file_name = f"dataset_{ALL_MONTHS[0]}-{ALL_MONTHS[-1]}"
target_file_name = f"dataset_{ALL_MONTHS[0] + fix_date}-{ALL_MONTHS[-1] + fix_date}"


utils.save_dataset(df_merge_2, target_file_name)