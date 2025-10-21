from pyspark.sql import functions as F, types as T
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import time
import sys
import os
from config_edit import config
import utils
import re

os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

def cal_feature_by_date(spark, DATE , table_name, feature_col, sub_col, fix_date_of_month):
    n_partitions = spark.sparkContext.defaultParallelism * 2
    print('Get data ...')
    if run_mode == 'prod':
        query = f"select * from {table_name_on_production} where s3_file_date ='{DATE}'"
    else: 
        query = f"select * from {table_name_on_production} WHERE s3_file_date = '{DATE}' and msisdn in (select {backtest_table_phone_name} from {backtest_table_name})"

    df = utils.spark_read_data_from_singlestore(spark, query)
    df = df.repartition(n_partitions)

    df = (
        df.withColumn(f'{feature_col}_square', F.pow(F.col(feature_col).cast(T.DoubleType()), 2))
    )
    df_agg_1 = (
        df.groupBy(['msisdn']).agg(
            F.sum(F.col(feature_col)).cast(T.DoubleType()).alias(f'{table_name}_sum_{feature_col}*{sub_col}*Filter=full'),             
            (F.sum(F.col(feature_col)) / F.count(F.col(feature_col))).alias(f'{table_name}_avg_{feature_col}*{sub_col}*Filter=full'),
            F.count(F.col(feature_col)).cast(T.DoubleType()).alias(f'{table_name}_count_{feature_col}*{sub_col}*Filter=full'),
            F.min(F.col(feature_col)).cast(T.DoubleType()).alias(f'{table_name}_min_{feature_col}*{sub_col}*Filter=full'),
            F.max(F.col(feature_col)).cast(T.DoubleType()).alias(f'{table_name}_max_{feature_col}*{sub_col}*Filter=full'),
            F.stddev(F.col(feature_col)).cast(T.DoubleType()).alias(f'{table_name}_std_{feature_col}*{sub_col}*Filter=full'),
            F.sum(F.col(feature_col+'_square')).cast(T.DoubleType()).alias(f'{table_name}_sumsquare_{feature_col}*{sub_col}*Filter=full'),
        )
    )
    df_agg_2 = (                                                                                                    
        df.filter(F.col('module') == 'RefillRecord').groupBy(['msisdn']).agg(
            F.sum(F.col(feature_col)).cast(T.DoubleType()).alias(f'{table_name}_sum_{feature_col}*{sub_col}*Filter=module==RefillRecord'),
            (F.sum(F.col(feature_col)) / F.count(F.col(feature_col))).alias(f'{table_name}_avg_{feature_col}*{sub_col}*Filter=module==RefillRecord'),            
            F.count(F.col(feature_col)).cast(T.DoubleType()).alias(f'{table_name}_count_{feature_col}*{sub_col}*Filter=module==RefillRecord'),
            F.min(F.col(feature_col)).cast(T.DoubleType()).alias(f'{table_name}_min_{feature_col}*{sub_col}*Filter=module==RefillRecord'),
            F.max(F.col(feature_col)).cast(T.DoubleType()).alias(f'{table_name}_max_{feature_col}*{sub_col}*Filter=module==RefillRecord'),
            F.stddev(F.col(feature_col)).cast(T.DoubleType()).alias(f'{table_name}_std_{feature_col}*{sub_col}*Filter=module==RefillRecord'),
            F.sum(F.col(feature_col+'_square')).cast(T.DoubleType()).alias(f'{table_name}_sumsquare_{feature_col}*{sub_col}*Filter=module==RefillRecord'),
        )
    )
    df_agg_3 = (
        df.filter(F.col('module') == 'AdjustmentRecord').groupBy(['msisdn']).agg(
            F.sum(F.col(feature_col)).cast(T.DoubleType()).alias(f'{table_name}_sum_{feature_col}*{sub_col}*Filter=module==AdjustmentRecord'),
            (F.sum(F.col(feature_col)) / F.count(F.col(feature_col))).alias(f'{table_name}_avg_{feature_col}*{sub_col}*Filter=module=AdjustmentRecord'),
            F.count(F.col(feature_col)).cast(T.DoubleType()).alias(f'{table_name}_count_{feature_col}*{sub_col}*Filter=module==AdjustmentRecord'),
            F.min(F.col(feature_col)).cast(T.DoubleType()).alias(f'{table_name}_min_{feature_col}*{sub_col}*Filter=module==AdjustmentRecord'),
            F.max(F.col(feature_col)).cast(T.DoubleType()).alias(f'{table_name}_max_{feature_col}*{sub_col}*Filter=module==AdjustmentRecord'),
            F.stddev(F.col(feature_col)).cast(T.DoubleType()).alias(f'{table_name}_std_{feature_col}*{sub_col}*Filter=module==AdjustmentRecord'),
            F.sum(F.col(feature_col+'_square')).cast(T.DoubleType()).alias(f'{table_name}_sumsquare_{feature_col}*{sub_col}*Filter=module==AdjustmentRecord'),
        )
    )
    
    # Join multiple dataframes
    df_agg_joined = df_agg_1 \
        .join(df_agg_2, 'msisdn', how = 'fullouter') \
        .join(df_agg_3, 'msisdn', how = 'fullouter') \

    # Save join result to S3
    print('Saving result ...')
    utils.save_to_s3_by_date(df_agg_joined, table_name, DATE, fix_date_of_month, run_mode)
    spark.sparkContext._jvm.System.gc()
    
    ### FINISH
    print(f'Finshed {DATE}')

def cal_features_by_month(spark, table_name, feature_col, sub_col, fix_date_of_month):
    print(f"------------------------   {fix_date_of_month}  ---------------------------")
        
    ### date range
    end_date = datetime.strptime(fix_date_of_month, "%Y%m%d")   
    start_date = end_date - relativedelta(months=1) + relativedelta(days=1)
    end_date_str = end_date.strftime("%Y%m%d")
    start_date_str = start_date.strftime("%Y%m%d")
    
    ### get data
    print('Get data ...')
    query = f"select distinct s3_file_date from {table_name_on_production} where s3_file_date BETWEEN '{start_date_str}' AND '{end_date_str}'"
    # Get date in month
    date_in_month = utils.spark_read_data_from_singlestore(spark, query)
    dates_in_month = date_in_month.toPandas().values                                   
    dates_in_month = list(map(lambda x: x[0], dates_in_month))
    for i, date in enumerate(dates_in_month):
        # if date in date_finished:
        #     print(f'Skip {date}')
        #     continue
        try:
            st = time.time()
            DATE = date
            cal_feature_by_date(spark, DATE , table_name, feature_col, sub_col, fix_date_of_month) 
            et = time.time()
            print(f'Time taked: {str((et - st) /60)} minutes')
        except:
            try:                
                st = time.time()
                DATE = date
                cal_feature_by_date(spark, DATE , table_name, feature_col, sub_col, fix_date_of_month)
                et = time.time()
                print(f'Time taked: {str((et - st) /60)} minutes')
            except:
                st = time.time()
                DATE = date
                cal_feature_by_date(spark, DATE , table_name, feature_col, sub_col, fix_date_of_month)
                et = time.time()
                print(f'Time taked: {str((et - st) /60)} minutes')
        date_finished.append(date)
    
    date_features_df = utils.load_from_s3_by_date(spark, table_name, fix_date_of_month, run_mode).where(f"date >= {start_date_str} and date <= {end_date_str}")
    full_date_df = date_features_df.select('msisdn').distinct().crossJoin(F.broadcast(date_features_df.select('date').distinct()))
    date_features_df = full_date_df.join(date_features_df, on = ['msisdn', 'date'], how = 'left')
    date_features_df = date_features_df.fillna(0.0)

    acc_date_features = (
        date_features_df.groupby("msisdn").agg(
                F.sum(F.col(f'{table_name}_sum_{feature_col}*{sub_col}*Filter=full')).alias(f'{table_name}_sum_{feature_col}*{sub_col}*Filter=full'),
                F.sum(F.col(f'{table_name}_count_{feature_col}*{sub_col}*Filter=full')).alias(f'{table_name}_count_{feature_col}*{sub_col}*Filter=full'),
                (F.sum(F.col(f'{table_name}_sum_{feature_col}*{sub_col}*Filter=full')) / F.sum(F.col(f'{table_name}_count_{feature_col}*{sub_col}*Filter=full'))).alias(f'{table_name}_avg_{feature_col}*{sub_col}*Filter=full'),
                F.min(F.col(f'{table_name}_min_{feature_col}*{sub_col}*Filter=full')).alias(f'{table_name}_min_{feature_col}*{sub_col}*Filter=full'),
                F.max(F.col(f'{table_name}_max_{feature_col}*{sub_col}*Filter=full')).alias(f'{table_name}_max_{feature_col}*{sub_col}*Filter=full'),
                F.sum(F.col(f'{table_name}_sumsquare_{feature_col}*{sub_col}*Filter=full')).alias(f'{table_name}_sumsquare_{feature_col}*{sub_col}*Filter=full'), 

                F.sum(F.col(f'{table_name}_sum_{feature_col}*{sub_col}*Filter=module==RefillRecord')).alias(f'{table_name}_sum_{feature_col}*{sub_col}*Filter=module==RefillRecord'),
                F.sum(F.col(f'{table_name}_count_{feature_col}*{sub_col}*Filter=module==RefillRecord')).alias(f'{table_name}_count_{feature_col}*{sub_col}*Filter=module==RefillRecord'),
                (F.sum(F.col(f'{table_name}_sum_{feature_col}*{sub_col}*Filter=module==RefillRecord')) / F.sum(F.col(f'{table_name}_count_{feature_col}*{sub_col}*Filter=module==RefillRecord'))).alias(f'{table_name}_avg_{feature_col}*{sub_col}*Filter=module==RefillRecord'),
                F.min(F.col(f'{table_name}_min_{feature_col}*{sub_col}*Filter=module==RefillRecord')).alias(f'{table_name}_min_{feature_col}*{sub_col}*Filter=module==RefillRecord'),
                F.max(F.col(f'{table_name}_max_{feature_col}*{sub_col}*Filter=module==RefillRecord')).alias(f'{table_name}_max_{feature_col}*{sub_col}*Filter=module==RefillRecord'),
                F.sum(F.col(f'{table_name}_sumsquare_{feature_col}*{sub_col}*Filter=module==RefillRecord')).alias(f'{table_name}_sumsquare_{feature_col}*{sub_col}*Filter=module==RefillRecord'), 

                F.sum(F.col(f'{table_name}_sum_{feature_col}*{sub_col}*Filter=module==AdjustmentRecord')).alias(f'{table_name}_sum_{feature_col}*{sub_col}*Filter=module==AdjustmentRecord'),
                F.sum(F.col(f'{table_name}_count_{feature_col}*{sub_col}*Filter=module==AdjustmentRecord')).alias(f'{table_name}_count_{feature_col}*{sub_col}*Filter=module==AdjustmentRecord'),
                (F.sum(F.col(f'{table_name}_sum_{feature_col}*{sub_col}*Filter=module==AdjustmentRecord')) / F.sum(F.col(f'{table_name}_count_{feature_col}*{sub_col}*Filter=module==AdjustmentRecord'))).alias(f'{table_name}_avg_{feature_col}*{sub_col}*Filter=module==AdjustmentRecord'),
                F.min(F.col(f'{table_name}_min_{feature_col}*{sub_col}*Filter=module==AdjustmentRecord')).alias(f'{table_name}_min_{feature_col}*{sub_col}*Filter=module==AdjustmentRecord'),
                F.max(F.col(f'{table_name}_max_{feature_col}*{sub_col}*Filter=module==AdjustmentRecord')).alias(f'{table_name}_max_{feature_col}*{sub_col}*Filter=module==AdjustmentRecord'),
                F.sum(F.col(f'{table_name}_sumsquare_{feature_col}*{sub_col}*Filter=module==AdjustmentRecord')).alias(f'{table_name}_sumsquare_{feature_col}*{sub_col}*Filter=module==AdjustmentRecord'),
        )
    )

    ## fix tam thoi bang spec value -1 
    acc_date_features = acc_date_features.withColumn(f'{table_name}_std_{feature_col}*{sub_col}*Filter=full', F.lit(-1))\
                                        .withColumn(f'{table_name}_std_{feature_col}*{sub_col}*Filter=module==RefillRecord', F.lit(-1))\
                                        .withColumn(f'{table_name}_std_{feature_col}*{sub_col}*Filter=module==AdjustmentRecord', F.lit(-1))
    
    # Save to S3
    print('Saving result ...')
    utils.save_to_s3(acc_date_features, table_name, f'/l1m/date={fix_date_of_month}', run_mode)  
    
    # Finish
    print(f'Finished {fix_date_of_month}')


# Run by month
def run_all_month(spark, table_name, feature_col, sub_col, ALL_MONTHS, fix_date):
    date_finished = []
    for MONTH in ALL_MONTHS:
        st = time.time()
        fix_date_of_month = MONTH + fix_date
        try:
            utils.load_from_s3(spark, table_name, f'/l1m/date={fix_date_of_month}', run_mode)
            print("Skip")
        except:
            cal_features_by_month(spark, table_name, feature_col, sub_col, fix_date_of_month)
        et = time.time()
        print(f'Time taked: {str((et - st)/60)} minutes')
        print('')

def load_feature_by_month_and_merge(spark, table_name, ALL_MONTHS, fix_date):
    fix_date_of_all_months = list(map(lambda x: x + fix_date, ALL_MONTHS))
    selected_months = ['T-1', 'T-2', 'T-3', 'T-4', 'T-5', 'T-6']
    @F.udf(returnType=T.TimestampType())
    def shift_n_month(date, shift_month):
        shift_date = date + relativedelta(months=shift_month) 
        return shift_date

    # First month
    month = selected_months[0]
    shift_month = 0
    print(f'Month {month} - shift {shift_month} months')
    if run_mode == 'prod': 
        df = utils.load_from_s3(spark, table_name, f'/l1m/date={fix_date_of_all_months[0]}',run_mode)
    else:
        df = utils.load_from_s3(spark, table_name, '/l1m/',run_mode)
    df_columns = df.columns
    df_columns_added_time = [f"`{x}` as `{x}_{month}`" if table_name in x else f"{x}" for x in df_columns]
    df = df.selectExpr(*df_columns_added_time)

    for i, month in enumerate(selected_months[1:]):
        shift_month = i + 1
        print(f'Month {month} - shift {shift_month} months')
        if run_mode == 'prod': 
            df_2 = utils.load_from_s3(spark, table_name, f'/l1m/date={fix_date_of_all_months[shift_month]}', run_mode)  
        else:
            df_2 = utils.load_from_s3(spark, table_name, '/l1m/',run_mode)
        df_2_columns = df_2.columns
        df_2_columns_added_time = [f"`{x}` as `{x}_{month}`" if table_name in x else f"{x}" for x in df_2_columns]
        df_2 = df_2.selectExpr(*df_2_columns_added_time)
        
        if run_mode == 'prod':
            df = df.join(df_2, "msisdn", how = 'outer')
        else:
            df_2 = df_2.withColumn('date', F.to_timestamp(F.col('date').cast(T.StringType()), 'yyyyMMdd'))
            df_2 = df_2.withColumn('shift_month', F.lit(shift_month))
            df_2 = df_2.withColumn('date', shift_n_month(F.col('date'), F.col('shift_month')))\
                       .withColumn("date", F.date_format("date", "yyyyMMdd"))

            df = df.join(df_2, ["msisdn", "date"], how = 'outer')
        df = df.fillna(0.0)
    df = df.checkpoint()
    
    # Rename
    selected_features = df.columns
    selected_df_with_label = df
    
    # Group feature level 1
    key_operators = ['sum', 'avg', 'count', 'min', 'max', 'std', 'sumsquare']
    def remove_key_operator(feature_name, key_operators):
        parts = feature_name.split('_')
        removed_operator_part = []
        for part in parts:
            if part not in key_operators:
                removed_operator_part.append(part)
        removed_operator_feature = "_".join(removed_operator_part)
        return removed_operator_feature
    removed_key_operator_features = list(map(lambda x: remove_key_operator(x, key_operators), selected_features ))
    group_feature_by_indexes = pd.Series(range(len(removed_key_operator_features))).groupby(removed_key_operator_features, sort = False).apply(list).tolist()

    # Group feature level 2
    removed_key_and_time_operator_features = list(map(lambda x: x[:-3] , removed_key_operator_features))
    group_feature_by_indexes_first = list(map(lambda x: x[0], group_feature_by_indexes  ))
    group_feature_representative = list(map(lambda x: removed_key_and_time_operator_features[x] , group_feature_by_indexes_first))
    group_feature_by_indexes_level2 = pd.Series(range(len(group_feature_representative))).groupby(group_feature_representative, sort = False).apply(list).tolist()
    group_feature_by_indexes_level_2 = []
    for group_level_2_index in group_feature_by_indexes_level2:
        group_level_2 = list(map(lambda x: group_feature_by_indexes[x] ,group_level_2_index ))
        group_feature_by_indexes_level_2.append(group_level_2)
    
    # Calculate accumulated features
    acc_months = ['T-1','T-2', 'T-3', 'T-4', 'T-5', 'T-6']
    group_names = []
    for group in group_feature_by_indexes_level_2:
        if len(group) == 1:
            continue
        # Group featues
        group_fetures = []
        for row in group:
            group_fetures.append(list(map(lambda x: selected_features[x], row )))

        # Group name
        group_name = "_".join(group_fetures[0][0][len(table_name) + 1:].split('_')[1:-1])
        print(f'Calculate group name: {group_name}')
        group_names.append(group_name)
        selected_df_with_label = (
            selected_df_with_label
            .withColumn(f'f_{table_name}_sum_{group_name}_T-1-acc', F.col(*[col for col in group_fetures[0] if re.search(r'_sum_', col)]))
            .withColumn(f'f_{table_name}_count_{group_name}_T-1-acc', F.col(*[col for col in group_fetures[0] if re.search(r'_count_', col)]))
            .withColumn(f'f_{table_name}_avg_{group_name}_T-1-acc', F.col(*[col for col in group_fetures[0] if re.search(r'_avg_', col)]))
            .withColumn(f'f_{table_name}_min_{group_name}_T-1-acc', F.col(*[col for col in group_fetures[0] if re.search(r'_min_', col)]))
            .withColumn(f'f_{table_name}_max_{group_name}_T-1-acc', F.col(*[col for col in group_fetures[0] if re.search(r'_max_', col)]))
            .withColumn(f'f_{table_name}_sumsquare_{group_name}_T-1-acc', F.col(*[col for col in group_fetures[0] if re.search(r'_sumsquare_', col)]))
            .withColumn(f'f_{table_name}_variance_{group_name}_T-1-acc', (F.col(f'f_{table_name}_sumsquare_{group_name}_T-1-acc') / F.col(f'f_{table_name}_count_{group_name}_T-1-acc')) - F.col(f'f_{table_name}_avg_{group_name}_T-1-acc') ** 2)
            .withColumn(f'f_{table_name}_std_{group_name}_T-1-acc', F.sqrt(F.col(f'f_{table_name}_variance_{group_name}_T-1-acc')))
        )
        for i, month in enumerate(acc_months[1:]):
            selected_df_with_label = (
                selected_df_with_label
                .withColumn(f'f_{table_name}_sum_{group_name}_{month}-acc', F.col(*[col for col in group_fetures[i+1] if re.search(r'_sum_', col)]) + F.col(f'f_{table_name}_sum_{group_name}_{acc_months[i]}-acc'))
                .withColumn(f'f_{table_name}_count_{group_name}_{month}-acc',  F.col(*[col for col in group_fetures[i+1] if re.search(r'_count_', col)]) + F.col(f'f_{table_name}_count_{group_name}_{acc_months[i]}-acc'))
                .withColumn(f'f_{table_name}_avg_{group_name}_{month}-acc', F.col(f'f_{table_name}_sum_{group_name}_{month}-acc') / F.col(f'f_{table_name}_count_{group_name}_{month}-acc'))
                .withColumn(f'f_{table_name}_min_{group_name}_{month}-acc', F.least(F.col(*[col for col in group_fetures[i+1] if re.search(r'_min_', col)]), F.col(f'f_{table_name}_min_{group_name}_{acc_months[i]}-acc')))
                .withColumn(f'f_{table_name}_max_{group_name}_{month}-acc', F.greatest( F.col(*[col for col in group_fetures[i+1] if re.search(r'_max_', col)]), F.col(f'f_{table_name}_max_{group_name}_{acc_months[i]}-acc')))
                .withColumn(f'f_{table_name}_sumsquare_{group_name}_{month}-acc', F.col(*[col for col in group_fetures[i+1] if re.search(r'_sumsquare_', col)]) + F.col(f'f_{table_name}_sumsquare_{group_name}_{acc_months[i]}-acc'))
                .withColumn(f'f_{table_name}_variance_{group_name}_{month}-acc', (F.col(f'f_{table_name}_sumsquare_{group_name}_{month}-acc') / F.col(f'f_{table_name}_count_{group_name}_{month}-acc')) - F.col(f'f_{table_name}_avg_{group_name}_{month}-acc') ** 2)
                .withColumn(f'f_{table_name}_std_{group_name}_{month}-acc', F.sqrt(F.col(f'f_{table_name}_variance_{group_name}_{month}-acc')))
                .withColumn(f'f_{table_name}_ratio_{group_name}_{month}-acc', F.col(f'f_{table_name}_sum_{group_name}_{month}-acc') / F.col(f'f_{table_name}_sum_{group_name}_T-1-acc'))
            )
            selected_df_with_label = selected_df_with_label.checkpoint()
    
    ## Filter unneccesary column
    filter_acc_features = list(filter(lambda x: (x.endswith('-acc') and (('_sum_' in x) or ('_count_' in x) or ('_avg_' in x) or ('_min_' in x) or ('_max_' in x) or ('_std_' in x) or ('_ratio_' in x ) ) ) or x in ['date', 'msisdn'] , selected_df_with_label.columns))
    filtered_selected_df_with_label = selected_df_with_label.select(filter_acc_features)
    
    # Rearrange numeric feature
    key_operator_features = []
    numeric_key_operator = ['sum', 'avg', 'count', 'min', 'max', 'std', 'sumsquare']
    for month in acc_months:
        for group_name in group_names:
            for operator in numeric_key_operator:
                f_name = f'f_{table_name}_{operator}_{group_name}_{month}-acc'
                key_operator_features.append(f_name)
            
    # Rearrange ratio feature 
    ratio_features = []
    for month in acc_months[1:]:
        for group_name in group_names:
            f_name = f'f_{table_name}_ratio_{group_name}_{month}-acc'
            ratio_features.append(f_name)

    # Rearrange features
    if run_mode == 'backtest': 
        rearrange_acc_features = ["msisdn", "date"] + key_operator_features + ratio_features
    else: 
        rearrange_acc_features = ["msisdn"] + key_operator_features + ratio_features
    rearranged_filtered_selected_df_with_label = selected_df_with_label.select(rearrange_acc_features)
    
    # Add index
    start_index = 130
    indexed_rearrange_acc_features = []
    for feature in rearrange_acc_features:
        if feature.startswith('f'):
            indexed_feature = f'{feature[0]}{start_index}{feature[1:-4]}'
            indexed_rearrange_acc_features.append(indexed_feature)
            start_index += 1
        else:
            indexed_feature = feature
            indexed_rearrange_acc_features.append(indexed_feature)
    lst_column_renamed = [f"`{x}` as `{y}`" for x, y in zip(rearrange_acc_features , indexed_rearrange_acc_features)]
    rearranged_filtered_selected_df_with_label = rearranged_filtered_selected_df_with_label.selectExpr(*lst_column_renamed)
    
    # Fill NA
    fill_na_stratery = {
        group_names[0]: {
            'sum': 0.0,
            'count': 0.0,
            'avg': 0.0,
            'min': 0.0,
            'max': 0.0,
            'std': 0.0,
            'ratio': 0.0,
        },
        group_names[1]: {
            'sum': 0.0,
            'count': 0.0,
            'avg': 0.0,
            'min': 0.0,
            'max': 0.0,
            'std': 0.0,
            'ratio': 0.0,
        },
        group_names[2]: {
            'sum': 0.0,
            'count': 0.0,
            'avg': 0.0,
            'min': 0.0,
            'max': 0.0,
            'std': 0.0,
            'ratio': 0.0,
        }
    }
    numeric_operarors = ['sum', 'count', 'avg', 'min', 'max', 'std', 'ratio']
    for group in group_names:
        print(f'Fill na group name: {group}')
        features = list(filter(lambda x: group in x , indexed_rearrange_acc_features  ))
        for operator in numeric_operarors:
            filtered_features = list(filter(lambda x: f'_{operator}_' in x , features  ))
            filled_value = [fill_na_stratery[group][operator]] * len(filtered_features)
            filled_value_dict = dict(zip(filtered_features, filled_value))
            rearranged_filtered_selected_df_with_label = rearranged_filtered_selected_df_with_label.fillna(filled_value_dict)
    
    # SAVE
    if run_mode == 'prod': 
        target_file_name = f"merged/date={fix_date_of_all_months[0]}"
    else: 
        target_file_name = f"merged"
    utils.save_to_s3(rearranged_filtered_selected_df_with_label, table_name, target_file_name, run_mode)


# Define table
table_name = 'blueinfo_ocs_air'
table_name_on_production = config.table_dict[table_name]
feature_col = 'reillamount'
sub_col = ''
date_finished = []

# define date 
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
    fix_date = fix_dates[0]
    is_table_finished = utils.check_table_finished(spark, table_name,  ALL_MONTHS, fix_date, run_mode) 
    if is_table_finished:
        print("Skip")
    else:
        run_all_month(spark, table_name, feature_col, sub_col, ALL_MONTHS, fix_date)
        load_feature_by_month_and_merge(spark, table_name, ALL_MONTHS, fix_date)
else: 
    for fix_date in fix_dates: 
        run_all_month(spark, table_name, feature_col, sub_col, ALL_MONTHS, fix_date)
    load_feature_by_month_and_merge(spark, table_name, ALL_MONTHS, fix_date)
