from pyspark.sql import SparkSession, functions as F, types as T
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
import time
import sys
import os
from config_edit import config
import utils

os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

@F.udf(returnType = T.MapType(T.StringType(), T.IntegerType()))
def get_frequent_dict_of_category(categories):
    value_counts = pd.Series(categories).value_counts(sort=True).to_dict()    
    return value_counts

@F.udf(returnType = T.StringType() )
def get_most_frequent_item_of_category(category):
    if type(category) != dict or category=={}:
        return None
    value_counts = pd.Series(category).sort_values(ascending=False)
    most_frequent_item = value_counts.index[0]
    return most_frequent_item

@F.udf(returnType = T.StringType())
def get_least_frequent_item_of_category(category):
    if type(category) != dict or category=={}:
        return None
    value_counts = pd.Series(category).sort_values(ascending=False)
    if len(value_counts) == 1:
        least_frequent_item = None
    else:
        least_frequent_item = value_counts.index[-1]
    return least_frequent_item

@F.udf(returnType = T.IntegerType())
def get_nunique_item_of_category(category):
    if type(category) != dict or category=={}:
        return 0
    value_counts = pd.Series(category).sort_values(ascending=False)
    unique = len(value_counts)
    return unique

@F.udf(returnType = T.IntegerType())
def get_most_frequent_value_of_category(category):
    if type(category) != dict or category=={}:
        return 0
    value_counts = pd.Series(category).sort_values(ascending=False)
    most_frequent_value = value_counts.values[0]    
    return int(most_frequent_value)

@F.udf(returnType = T.FloatType())
def get_most_frequent_percent_of_category(category):
    if type(category) != dict or category=={}:
        return float(0)
    value_counts = pd.Series(category).sort_values(ascending=False)
    most_frequent_value = value_counts.values[0]    
    most_frequent_percent = most_frequent_value /  value_counts.values.sum()
    return float(most_frequent_percent)

@F.udf(returnType = T.IntegerType())
def get_least_frequent_value_of_category(category):
    if type(category) != dict or category=={}:
        return 0
    value_counts = pd.Series(category).sort_values(ascending=False)
    if len(value_counts) == 1:
        least_frequent_value = 0   
    else:
        least_frequent_value = value_counts.values[-1]    
    return int(least_frequent_value)

@F.udf(returnType = T.FloatType())
def get_least_frequent_percent_of_category(category):
    if type(category) != dict or category=={}:
        return float(0) 
    value_counts = pd.Series(category).sort_values(ascending=False)
    if len(value_counts) == 1:
        least_frequent_percent = float(0)
    else:
        least_frequent_value = value_counts.values[-1]    
        least_frequent_percent = float(least_frequent_value / value_counts.values.sum())
    return least_frequent_percent

@F.udf(returnType = T.MapType(T.StringType(), T.IntegerType() ))
def join_2_category(category_1, category_2):
    if type(category_1) != dict and type(category_2) == dict:
        return category_2
    elif type(category_1) == dict and type(category_2) != dict:
        return category_1
    elif type(category_1) != dict and type(category_2) != dict:
        return {}
    else:
        keys = np.unique(list(category_1.keys()) + list(category_2.keys()))
        merged_category = {}
        for key in keys:
            merged_category[str(key)] = int((category_1[key] if key in category_1 else 0) + (category_2[key] if key in category_2 else 0))
        return merged_category
    
@F.udf(returnType=T.StringType())
def rename_sender(sender):
    renamed_sender = sender.upper()
    renamed_sender = "".join(list(filter(lambda x: x != ' ', list(renamed_sender) )))
    return renamed_sender

def rename_sender_text(sender):
    renamed_sender = sender.upper()
    renamed_sender = "".join(list(filter(lambda x: x != ' ', list(renamed_sender) )))
    return renamed_sender

finance_name = """
BACABANK
LOTTE_FIN
BVBank
Sacombank
SHB_FINANCE
infina
AGRIBANK-KT
FINVIET
HDBank
EVNFinance
BANKPLUS
Techcombank
Citibank VN
Co-opBank
LPBank
VietinBank
BAOVIETBank
MeeyFinance
Vietcombank
VietABank
AGRIBANK
Liobank
CBBank
SeABank
PGBank
MBBANK
MBBANKPLUS
ShinhanBank
DongA Bank
SFIN
PVcomBank
SHBFinance
PublicBank
OCEANBANK
NamABank
Eximbank
FinInterAMC
FinMart
FinWorld
CIMB BANK
VIETBANK
SaiGonBank
GPBANK
TNEXFinance
ABBANK
KBank HCMC
Agribank_LD
VPBank
TPBank
KSFinance
CITIBANK
VPBank
GFIN VN
KB FINA
FINBOX
FINPATH
AGRIBANK_HD
BANKTCKCB
FINHAY_VN
BIFIN
Gembank VN
AGRIBANK_NX
EduFin
DBS Bank
EUPFIN VN
ONEFIN
INFINIQVN
Affina
BanVietBank
Bifin.vn
BankSinoPac
SCB
HVB
UOB
LPB
SHB
OCB
ACB
VRB
IVB
MSB
VIB
TVB
VLB
HDB
CTY 9PAY
Papaya.asia
ZaloPay
myCityPay
ECOFARMPAY
VI VNPAY
PAYOO
SMARTPAY
VNPAY-QR
PayME
ShopeePay
VNPAY-TAXI
VTPAY
VNPAY
SENPAY
GPAY
OMIPAY.VN
M-PAY
Galaxy Pay
IPay.vn
WEPAY
DIGIPAY
VNPT EPAY
VITAPAY.VN
YOPAYMENT
VNPAY-POS
VIETTELPAY
PAYPAL
EPAYJSC
UnionPay
PAYTRIP
ONEPAY
MOMO
HomeCredit
MCREDIT
VietCredit
VNDCREDIT
EASY CREDIT
FE CREDIT
FECREDIT
Fundiin
MicroFund
AnBinh_Fund
DSGFund
HATA INVEST
DIGI INVEST
SmartInvest
KITA INVEST
ICEinvest
CenInvest
HUD INVEST
H9BC_INVEST
FICO INVEST
VinaCapital
MB Capital
PVCBCAPITAL
CAPITALAND
VW Capital
HDCAPITAL
VTBCAPITAL
LHCapital
VOI CAPITAL
CILC-LEASE
VCBLEASING
GM Holdings
TNRHoldings
PNHOLDING
Seaholdings
"""
finance_list = finance_name.splitlines()
finance_list = list(map(lambda x: rename_sender_text(x), finance_list[1:] ))

def cal_features_by_month(spark, table_name, feature_col, sub_col, fix_date_of_month):
    print(f"------------------------   {fix_date_of_month}  ---------------------------")
    
    # date range
    end_date = datetime.strptime(fix_date_of_month, "%Y%m%d")   
    start_date = end_date - relativedelta(months=1) + relativedelta(days=1)
    end_date_str = end_date.strftime("%Y%m%d")
    start_date_str = start_date.strftime("%Y%m%d")
    
    # get data
    print('Get data ...')
    if run_mode == "prod":
        query = f"select * from {table_name_on_production} where s3_file_date BETWEEN '{start_date_str}' AND '{end_date_str}'"
    else:
        query = f"select * from {table_name_on_production} where s3_file_date BETWEEN '{start_date_str}' AND '{end_date_str}' AND SENDER in (select {backtest_table_phone_name} from {backtest_table_name})"

    df = utils.spark_read_data_from_singlestore(spark, query)
    
    df = df.withColumn('SENDER', rename_sender(F.col('SENDER')))    
    df_agg_1 = (
        df
        .groupBy(['msisdn'])
        .agg(
            F.collect_list(feature_col).alias(f'{table_name}_list_{feature_col}*{sub_col}*Filter=full'),
        )
        .withColumn(f'{table_name}_frequentdict_{feature_col}*{sub_col}*Filter=full', get_frequent_dict_of_category(F.col(f'{table_name}_list_{feature_col}*{sub_col}*Filter=full')))
        .drop(f'{table_name}_list_{feature_col}*{sub_col}*Filter=full')
    )
    df_agg_2 = (
        df
        .filter(df['SENDER'].isin(finance_list))
        .groupBy(['msisdn'])
        .agg(
            F.collect_list(feature_col).alias(f'{table_name}_list_{feature_col}*{sub_col}*Filter=finance'),
        )
        .withColumn(f'{table_name}_frequentdict_{feature_col}*{sub_col}*Filter=finance', get_frequent_dict_of_category(F.col(f'{table_name}_list_{feature_col}*{sub_col}*Filter=finance')))
        .drop(f'{table_name}_list_{feature_col}*{sub_col}*Filter=finance')
    ) 

    # Join
    print('Join ...')
    df_agg_joined_1 = (
        df_agg_1.withColumnRenamed("msisdn", "msisdn_df1").join(df_agg_2.withColumnRenamed("msisdn", "msisdn_df2"), F.col('msisdn_df1') == F.col('msisdn_df2'), how ="fullouter")
        .withColumn('msisdn_new', F.coalesce("msisdn_df1", "msisdn_df2"))
        .drop('msisdn_df1', 'msisdn_df2')
        .withColumnRenamed("msisdn_new", "msisdn")
    )

    # save to s3
    print('Save ...')
    utils.save_to_s3(df_agg_joined_1, table_name, f'/l1m/date={fix_date_of_month}', run_mode)
    
    # finish
    print(f'FINISH {fix_date_of_month}')
    
# RUN BY MONTH
def run_all_month(spark, table_name, feature_col, sub_col, ALL_MONTHS, fix_date):
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

def load_feature_by_month_and_merge(spark, table_name, feature_col, sub_col, ALL_MONTHS, fix_date):
    fix_date_of_all_months = list(map(lambda x: x + fix_date, ALL_MONTHS))
    selected_months = ['T-1', 'T-2', 'T-3', 'T-4', 'T-5', 'T-6']
    @F.udf(returnType=T.TimestampType())
    def shift_n_month(date, shift_month):
        shift_date = date + relativedelta(months=shift_month) 
        return shift_date

    # first month
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

    # rename
    selected_features = df.columns
    selected_df_with_label = df 
        
    ##### group feature level 1
    key_operators = ['frequentdict']
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

    ### group feature level 2
    removed_key_and_time_operator_features = list(map(lambda x: x[:-3] , removed_key_operator_features))
    group_feature_by_indexes_first = list(map(lambda x: x[0], group_feature_by_indexes  ))
    group_feature_representative = list(map(lambda x: removed_key_and_time_operator_features[x] , group_feature_by_indexes_first))
    group_feature_by_indexes_level2 = pd.Series(range(len(group_feature_representative))).groupby(group_feature_representative, sort = False).apply(list).tolist()
    group_feature_by_indexes_level_2 = []
    for group_level_2_index in group_feature_by_indexes_level2:
        group_level_2 = list(map(lambda x: group_feature_by_indexes[x] ,group_level_2_index ))
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
            .withColumn(f'{table_name}_frequentdict_{group_name}_T-1-acc', F.col(f'{table_name}_frequentdict_{group_name}_T-1' , ) )
            .withColumn(f'f_{table_name}_least-frequent-item_{group_name}_T-1-acc', get_least_frequent_item_of_category(F.col(group_fetures[0][0]))   )
            .withColumn(f'f_{table_name}_most-frequent-item_{group_name}_T-1-acc', get_most_frequent_item_of_category(F.col(group_fetures[0][0]))  )
            .withColumn(f'f_{table_name}_unique_{group_name}_T-1-acc',  get_nunique_item_of_category(F.col(group_fetures[0][0])) )
            .withColumn(f'f_{table_name}_most-frequent-value_{group_name}_T-1-acc', get_most_frequent_value_of_category(F.col(group_fetures[0][0])) )
            .withColumn(f'f_{table_name}_least-frequent-value_{group_name}_T-1-acc', get_least_frequent_value_of_category(F.col(group_fetures[0][0])) )
            .withColumn(f'f_{table_name}_most-frequent-percent_{group_name}_T-1-acc', get_most_frequent_percent_of_category(F.col(group_fetures[0][0])) )
            .withColumn(f'f_{table_name}_least-frequent-percent_{group_name}_T-1-acc', get_least_frequent_percent_of_category(F.col(group_fetures[0][0])) )
        )
        for i, month in enumerate(acc_months[1:]):
            selected_df_with_label = (
                selected_df_with_label
                .withColumn(f'{table_name}_frequentdict_{group_name}_{month}-acc', join_2_category( F.col(f'{table_name}_frequentdict_{group_name}_{month}') , F.col(f'{table_name}_frequentdict_{group_name}_{acc_months[i]}-acc')  ) )
                .withColumn(f'f_{table_name}_least-frequent-item_{group_name}_{month}-acc', get_least_frequent_item_of_category(F.col(f'{table_name}_frequentdict_{group_name}_{month}-acc')) )
                .withColumn(f'f_{table_name}_most-frequent-item_{group_name}_{month}-acc',  get_most_frequent_item_of_category(F.col(f'{table_name}_frequentdict_{group_name}_{month}-acc')) )
                .withColumn(f'f_{table_name}_unique_{group_name}_{month}-acc', get_nunique_item_of_category(F.col(f'{table_name}_frequentdict_{group_name}_{month}-acc')) )
                .withColumn(f'f_{table_name}_most-frequent-value_{group_name}_{month}-acc', get_most_frequent_value_of_category(F.col(f'{table_name}_frequentdict_{group_name}_{month}-acc') )  )
                .withColumn(f'f_{table_name}_least-frequent-value_{group_name}_{month}-acc', get_least_frequent_value_of_category(F.col(f'{table_name}_frequentdict_{group_name}_{month}-acc') )) 
                .withColumn(f'f_{table_name}_most-frequent-percent_{group_name}_{month}-acc', get_most_frequent_percent_of_category(F.col(f'{table_name}_frequentdict_{group_name}_{month}-acc') ) )
                .withColumn(f'f_{table_name}_least-frequent-percent_{group_name}_{month}-acc', get_least_frequent_percent_of_category(F.col(f'{table_name}_frequentdict_{group_name}_{month}-acc')  ))
            )

    ## Filter unneccesary column
    filter_acc_features = list(filter(lambda x: (x.startswith('f_') and x.endswith('-acc') and (('_least-frequent-item_' in x) or ('_most-frequent-item_' in x) or ('_unique_' in x) or ('_most-frequent-value_' in x) or ('_least-frequent-value_' in x) or ('_most-frequent-percent_' in x) or ('_least-frequent-percent_' in x ) ) ) or x in ['date', 'msisdn'] , selected_df_with_label.columns))
    filtered_selected_df_with_label = selected_df_with_label.select(filter_acc_features)
    
    ## Rearrange and add index
    rearrange_acc_features = filter_acc_features
    rearranged_filtered_selected_df_with_label = filtered_selected_df_with_label.select(rearrange_acc_features)
    indexed_rearrange_acc_features = rearrange_acc_features
    for f_old, f_new in zip(rearrange_acc_features , indexed_rearrange_acc_features):
        rearranged_filtered_selected_df_with_label = rearranged_filtered_selected_df_with_label.withColumnRenamed(f_old, f_new)
    
    # Fill NA
    fill_na_stratery = {
        group_names[0]: {
            'least-frequent-item': 'null',
            'most-frequent-item': 'null',
            'unique': 0,
            'most-frequent-value': 0,
            'least-frequent-value': 0,
            'most-frequent-percent': 0,
            'least-frequent-percent': 0,
        },
        group_names[1]: {
            'least-frequent-item': 'null',
            'most-frequent-item': 'null',
            'unique': 0,
            'most-frequent-value': 0,
            'least-frequent-value': 0,
            'most-frequent-percent': 0,
            'least-frequent-percent': 0,
        }
    }
    category_operarors = ['least-frequent-item', 'most-frequent-item', 'unique', 'most-frequent-value', 'least-frequent-value', 'most-frequent-percent', 'least-frequent-percent']
    for group in group_names:
        print(f'Fill na group name: {group}')
        features = list(filter(lambda x: group in x , indexed_rearrange_acc_features  ))
        for operator in category_operarors:
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
table_name = 'blueinfo_vascdr_brandname_meta'
table_name_on_production = config.table_dict[table_name]
feature_col = 'SENDER'
sub_col = ''

# Define date
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
        load_feature_by_month_and_merge(spark, table_name, feature_col, sub_col, ALL_MONTHS, fix_date)
else: 
    for fix_date in fix_dates: 
        run_all_month(spark, table_name, feature_col, sub_col, ALL_MONTHS, fix_date)
    load_feature_by_month_and_merge(spark, table_name, feature_col, sub_col, ALL_MONTHS, fix_date)