from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window
from dateutil.relativedelta import relativedelta
from datetime import datetime
import pandas as pd
import numpy as np
import time
import sys
import os
from config_edit import config
import utils
import re

os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'


def cal_features_by_month(spark, feature_list, sub_col, MONTH, table_name, fix_date_of_month):
    print(f"------------------------   {fix_date_of_month}  ---------------------------")
    
    # date range
    end_date = datetime.strptime(fix_date_of_month, "%Y%m%d") 
    start_date = end_date - relativedelta(months=1) + relativedelta(days=1)
    end_date_str = end_date.strftime("%Y%m%d")
    start_date_str = start_date.strftime("%Y%m%d")
    
    # get data
    print('Getting data ...')
    for feature in feature_list:
        feature_col = feature
        print(f'Feature: {feature_col}')
        st = time.time()

        if run_mode == "prod":
            query = f"select ACCS_MTHD_KEY, {feature}, s3_file_date from {table_name_on_production} where s3_file_date BETWEEN '{start_date_str}' AND '{end_date_str}'"
        else:
            query = f"select ACCS_MTHD_KEY, {feature}, s3_file_date from {table_name_on_production} where s3_file_date BETWEEN '{start_date_str}' AND '{end_date_str}' AND ACCS_MTHD_KEY in (select {backtest_table_phone_name} from {backtest_table_name})"
        
        df = utils.spark_read_data_from_singlestore(spark, query)
        df = df.withColumnRenamed("ACCS_MTHD_KEY", "msisdn")
        windowSpec = Window.partitionBy('msisdn').orderBy(sub_col)
        if int(fix_date_of_month[:6]) < 202403:     ####### Truoc thang 3, 1 sdt chi co 1 ban ghi /ngay; tu thang 3, 1 sdt co nhieu ban ghi/ngay
            df = df.withColumn("lag", F.lag(feature_col, 1).over(windowSpec)).withColumn("delta", F.abs(F.col(feature_col) - F.col("lag")))
            df = df.withColumn("delta", F.coalesce(F.col("delta"), F.col(feature_col)))
            df = df.filter("delta > 0").select('msisdn', F.col("delta").alias(feature_col))
        else:
            df = df.select('msisdn', feature_col, sub_col).groupBy('msisdn', sub_col).agg(F.sum(feature_col).alias(feature_col))
            df = df.withColumn("lag", F.lag(feature_col, 1).over(windowSpec)).withColumn("delta", F.abs(F.col(feature_col) - F.col("lag")))
            df = df.withColumn("delta", F.coalesce(F.col("delta"), F.col(feature_col)))
            df = df.filter("delta > 0").select('msisdn', F.col("delta").alias(feature_col))
        df = df.withColumn(feature_col, df[feature_col].cast(T.DoubleType()))
        df = df.withColumn(f'{feature_col}_square', F.pow(F.col(feature_col).cast(T.DoubleType()), 2))
        df_agg_1 = (
            df.groupBy(['msisdn']).agg(
                F.sum(F.col(feature_col)).cast(T.DoubleType()).alias(f'{table_name}_sum_{feature_col}*{sub_col}*Filter=full'),
                F.avg(F.col(feature_col)).cast(T.DoubleType()).alias(f'{table_name}_avg_{feature_col}*{sub_col}*Filter=full'),
                F.count(F.col(feature_col)).cast(T.DoubleType()).alias(f'{table_name}_count_{feature_col}*{sub_col}*Filter=full'),
                F.min(F.col(feature_col)).cast(T.DoubleType()).alias(f'{table_name}_min_{feature_col}*{sub_col}*Filter=full'),
                F.max(F.col(feature_col)).cast(T.DoubleType()).alias(f'{table_name}_max_{feature_col}*{sub_col}*Filter=full'),
                F.stddev(F.col(feature_col)).cast(T.DoubleType()).alias(f'{table_name}_std_{feature_col}*{sub_col}*Filter=full'),
                F.sum(F.col(feature_col+'_square')).cast(T.DoubleType()).alias(f'{table_name}_sumsquare_{feature_col}*{sub_col}*Filter=full'),
            )
        )

        # save by month
        print('Saving ...')        
        utils.save_to_s3(df_agg_1, table_name + part_number , f'/l1m/date={fix_date_of_month}/by_cols={feature}', run_mode)
    print(f'FINISH {MONTH}')

# Run by month
def run_all_month(spark, table_name, feature_list, sub_col, ALL_MONTHS, fix_date):
    for MONTH in ALL_MONTHS:
        st = time.time()
        fix_date_of_month = MONTH + fix_date
        cal_features_by_month(spark, feature_list, sub_col, MONTH, table_name, fix_date_of_month)
        et = time.time()
        print(f'Time taked: {str((et - st)/60)} minutes')
        print('')

# review
def load_feature_by_month_and_merge(spark, table_name, feature_list, fix_date, ALL_MONTHS):
    fix_date_of_all_months = list(map(lambda x: x + fix_date, ALL_MONTHS))
    for feature in feature_list:
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
            df = utils.load_from_s3(spark, table_name + part_number , f'/l1m/date={fix_date_of_all_months[0]}/by_cols={feature}', run_mode)
        else:
            df = utils.load_from_s3(spark, table_name + part_number , '/l1m/', run_mode, merge_schema = 'true').where(f"by_cols='{feature}'")
            columns = ['msisdn', 'date'] + [col for col in df.columns if feature in col]
            df = df.select(*columns)

        # df_columns = df.columns
        # df_columns_added_time = [f"`{x}` as `{x}_{month}`" if table_name in x else f"{x}" for x in df_columns]
        # df = df.selectExpr(*df_columns_added_time)

        # # Merge with y_label on backtest
        df_columns = df.columns
        df_columns_added_time = [f"`{x}` as `{x}_{month}`" if table_name in x else f"{x}" for x in df_columns]
        df = df.selectExpr(*df_columns_added_time)
        for i, month in enumerate(selected_months[1:]):
            shift_month = i + 1
            print(f'Month {month} - shift {shift_month} months')
            if run_mode == 'prod': 
                df_2 = utils.load_from_s3(spark, table_name + part_number , f'/l1m/date={fix_date_of_all_months[shift_month]}/by_cols={feature}', run_mode)
            else:
                df_2 = utils.load_from_s3(spark, table_name + part_number , '/l1m/', run_mode, merge_schema = 'true').where(f"by_cols='{feature}'")
                columns = ['msisdn', 'date'] + [col for col in df_2.columns if feature in col]
                df_2 = df_2.select(*columns)

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
        
        ### rename
        selected_features = df.columns
        selected_df_with_label = df
            
        ##### group feature level 1
        key_operators = ['sum', 'avg', 'count', 'min', 'max', 'std', 'sumsquare']
        def remove_key_operator(feature_name, key_operators):
            parts = feature_name.split('_')
            removed_operator_part = []
            for part in parts:
                if part not in key_operators:
                    removed_operator_part.append(part)
            removed_operator_feature = "_".join(removed_operator_part)
            return removed_operator_feature
        removed_key_operator_features = list(map(lambda x: remove_key_operator(x, key_operators), selected_features))
        group_feature_by_indexes = pd.Series(range(len(removed_key_operator_features))).groupby(removed_key_operator_features, sort = False).apply(list).tolist()

        ### group feature level 2
        removed_key_and_time_operator_features = list(map(lambda x: x[:-3] , removed_key_operator_features))
        group_feature_by_indexes_first = list(map(lambda x: x[0], group_feature_by_indexes))
        group_feature_representative = list(map(lambda x: removed_key_and_time_operator_features[x] , group_feature_by_indexes_first))
        group_feature_by_indexes_level2 = pd.Series(range(len(group_feature_representative))).groupby(group_feature_representative, sort = False).apply(list).tolist()
        group_feature_by_indexes_level_2 = []
        for group_level_2_index in group_feature_by_indexes_level2:
            group_level_2 = list(map(lambda x: group_feature_by_indexes[x] ,group_level_2_index))
            group_feature_by_indexes_level_2.append(group_level_2)
        
        ### Calculate accumulated features
        acc_months = ['T-1','T-2', 'T-3', 'T-4', 'T-5', 'T-6']
        group_names = []
        for group in group_feature_by_indexes_level_2:
            if len(group) == 1:
                continue
            
            # group featues
            group_fetures = []
            for row in group:
                group_fetures.append(list(map(lambda x: selected_features[x], row )))

            # group name
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
        filter_acc_features = list(filter(lambda x: (x.endswith('-acc') and (('_sum_' in x) or ('_count_' in x) or ('_avg_' in x) or ('_min_' in x) or ('_max_' in x) or ('_std_' in x) or ('_ratio_' in x))) or x in ['date', 'msisdn'] , selected_df_with_label.columns))
        filtered_selected_df_with_label = selected_df_with_label.select(filter_acc_features)

        ## Rearrange and add index
        # numeric feature
        key_operator_features = []
        numeric_key_operator = ['sum', 'avg', 'count', 'min', 'max', 'std', 'sumsquare']
        for month in acc_months:
            for group_name in group_names:
                for operator in numeric_key_operator:
                    f_name = f'f_{table_name}_{operator}_{group_name}_{month}-acc'
                    key_operator_features.append(f_name)
                
        # ratio feature 
        ratio_features = []
        for month in acc_months[1:]:
            for group_name in group_names:
                f_name = f'f_{table_name}_ratio_{group_name}_{month}-acc'
                ratio_features.append(f_name)
        
        # rearrange feature
        if run_mode == 'backtest': 
            rearrange_acc_features = ["msisdn", "date"] + key_operator_features + ratio_features
        else: 
            rearrange_acc_features = ["msisdn"] + key_operator_features + ratio_features
        rearranged_filtered_selected_df_with_label = selected_df_with_label.select(rearrange_acc_features)

        # Add index
        indexed_rearrange_acc_features = rearrange_acc_features
        lst_column_renamed = [f"`{x}` as `{y}`" for x, y in zip(rearrange_acc_features , indexed_rearrange_acc_features)]
        rearranged_filtered_selected_df_with_label = rearranged_filtered_selected_df_with_label.selectExpr(*lst_column_renamed)

        # Fill NA
        rearranged_filtered_selected_df_with_label = rearranged_filtered_selected_df_with_label.fillna(0)

        # Save
        if run_mode == "prod":
            target_file_name = f"sub_merged/date={fix_date_of_all_months[0]}/by_cols={feature}"
        else:
            target_file_name = f"sub_merged/by_cols={feature}"
        utils.save_to_s3(rearranged_filtered_selected_df_with_label, table_name + part_number , target_file_name, run_mode)

# Join feature by month
def join_features_by_month(spark, feature_list, table_name, fix_date, ALL_MONTHS):
    fix_date_of_all_months = list(map(lambda x: x + fix_date, ALL_MONTHS))
    if run_mode == "prod":
        df = utils.load_from_s3(spark, table_name + part_number , f'sub_merged/date={fix_date_of_all_months[0]}/by_cols={feature_list[0]}')
        for i, feature in enumerate(feature_list[1:]):
            df2 = utils.load_from_s3(spark, table_name + part_number , f'sub_merged/date={fix_date_of_all_months[0]}/by_cols={feature}')
            df = df.join(df2, 'msisdn', how = 'fullouter')
            if i % 5 == 0:
                df = df.checkpoint()
        print(f'Finished join {len(df.columns)} features')
    
        # save by month
        target_file_name = f"merged/date={fix_date_of_all_months[0]}"
    
    else:
        df = utils.load_from_s3(spark, table_name + part_number , f'sub_merged/by_cols={feature_list[0]}', run_mode)
        for i, feature in enumerate(feature_list[1:]):
            df2 = utils.load_from_s3(spark, table_name + part_number , f'sub_merged/by_cols={feature}', run_mode)
            df = df.join(df2, ['msisdn', 'date'], how = 'fullouter')
            if i % 5 == 0:
                df = df.checkpoint()
        print(f'Finished join {len(df.columns)} features')
    
        # save by month
        target_file_name = f"merged"

    utils.save_to_s3(df, table_name + part_number , target_file_name, run_mode)

# define date
ALL_MONTHS = config.ALL_MONTHS
ALL_MONTHS.sort(reverse = True)
fix_dates = config.fix_date
run_mode = config.run_mode

part_number = "_part3"
table_name = 'blueinfo_smrs_dwd_geo_rvn_mtd'
table_name_on_production = config.table_dict[table_name]
feature_list = [
    'TOT_SMS_OFFNET_IC_RVN_AMT',
    'TOT_SMS_OFFNET_OG_RVN_AMT',
    'TOT_SMS_ISD_OG_RVN_AMT',
    'TOT_SMS_ISD_IC_RVN_AMT',
    'TOT_MMS_RVN_AMT',
    'TOT_MMS_REFUND_AMT',
    'TOT_MMS_DISC_AMT',
    'TOT_MMS_RC_RVN_AMT',
    'TOT_MMS_NRC_RVN_AMT',
    'TOT_MMS_UC_RVN_AMT',
    'TOT_BUNDLE_RC_RVN_AMT',
    'TOT_BUNDLE_NRC_RVN_AMT',
    'TOT_VAS_RVN_AMT',
    'TOT_VAS_REFUND_AMT',
    'TOT_VAS_DISC_AMT',
    'TOT_DISC_AWARDED_AMT',
    'TOT_OTHRS_RVN_AMT',
    'TOT_OTHERS_RC_RVN_AMT',
    'TOT_OTHERS_NRC_RVN_AMT',
    'SMS_A2P_CNT'
]
sub_col = 's3_file_date'
backtest_table_name = config.backtest_table_dict[table_name]['backtest_table_name']
backtest_table_phone_name = config.backtest_table_dict[table_name]['backtest_table_phone_name']

# create Spark session
spark = utils.create_spark_instance(run_mode)

if run_mode == 'prod': 
    fix_date = fix_dates[0]
    is_table_finished = utils.check_table_finished(spark, table_name+part_number,  ALL_MONTHS, fix_date, run_mode) 
    if is_table_finished:
        print("Skip")
    else:
        run_all_month(spark, table_name, feature_list, sub_col, ALL_MONTHS, fix_date)
        load_feature_by_month_and_merge(spark, table_name, feature_list, fix_date, ALL_MONTHS)
        join_features_by_month(spark, feature_list, table_name, fix_date, ALL_MONTHS)
else: 
    for fix_date in fix_dates: 
        run_all_month(spark, table_name, feature_list, sub_col, ALL_MONTHS, fix_date)
    load_feature_by_month_and_merge(spark, table_name, feature_list, fix_date, ALL_MONTHS)
    join_features_by_month(spark, feature_list, table_name, fix_date, ALL_MONTHS)
