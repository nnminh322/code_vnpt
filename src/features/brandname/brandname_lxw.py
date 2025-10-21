import re
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql import Window
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta, MO
import pandas as pd
import numpy as np
import time
import sys
from src.utils import common as utils 
from src.utils.job_quality_utils import status_check, run_create_feature

import os
os.environ["LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN"] = "1"

brand_cat_expr = """
    CASE 
        WHEN REGEXP_LIKE (sender, 'stock|bank|credit|finance|papaya.asia|cathay|baohiem|bhxh|prudential|generali|manulife|aia vietnam|sun life|shinhanlife|chubb life|phuhunglife|pay') THEN 'finance'
        WHEN REGEXP_LIKE (sender, '^(momo|moca|vimo|hsc|tcbs|ssi|vps|vndirect|vix|vcbs|mas|vcsc|fpts|mbs|kis|tps|bvsc|shs|acbs|kbsv|bsc|vdsc|apg|ubcknn)$') THEN 'finance'
        WHEN REGEXP_LIKE (sender, '^(lotte_fin|infina|finviet|sfin|fininteramc|finworld|gfinvn|kbfina|finbox|finpath|finhay_vn|bifin|edufin|eupfinvn|onefin|infiniqvn|affina|bifin.vn|scb|hvb|uob|lpb|shb|ocb)$') THEN 'finance'
        WHEN REGEXP_LIKE (sender, '^(acb|vrb|ivb|msb|vib|tvb|vlb|hdb|fundiin|microfund|anbinh_fund|dsgfund|hatainvest|digiinvest|kitainvest|iceinvest|ceninvest|hudinvest|h9bc_invest|ficoinvest|vinacapital)$') THEN 'finance'
        WHEN REGEXP_LIKE (sender, '^(mbcapital|pvcbcapital|capitaland|vwcapital|hdcapital|vtbcapital|lhcapital|voicapital|cilc-lease|vcbleasing|gmholdings|tnrholdings|pnholding|seaholdings)$') THEN 'finance'
        WHEN REGEXP_LIKE (sender, 'shopee|tiki|tiktok|amazon|bigc|nguyenkim|circle k|gs25|bachhoaxanh|tgdd|didongviet|the\s?gioi|ministop|aeon|mart|vincom|mall|service') THEN 'spending'
        WHEN REGEXP_LIKE (sender, 'cinema|cgv|htv|vtv|thvl|^Zing$|^vng$|garena|vieon|sctv|^ghn$|express|vexere|agoda|airline|vietjet|tour|hk bamboo|vinasun|taxi|^grab$|^gojek$|xanh\s?sm|bus|^uber') THEN 'spending'
        WHEN REGEXP_LIKE (sender, 'vinpearl|muongthanh|hotel|holiday|resort|spa$|vacation|gongcha|cafe|coffe|phuc long|tocotoco|lotteria|kfc|jollibee|gogi|kichikichi|bbq|manwah|highland|pizza|popeyes|tuctactea|zentea|vannamtea|zen tea|phuc tea|trungnguyen') THEN 'spending'
        WHEN REGEXP_LIKE (sender, 'congan|hdnd|ub|ttdk|ttdk|vp bo tttt|hdnd|htx|sotuphap|thue|^evn|gas') THEN 'public_service'
        WHEN REGEXP_LIKE (sender, 'education|^thcs|^thpt|caodang|dai hoc|^hocvien|khtn|dhkt|dhnn|dhnl|tieuhoc|ngoaingu|PTNangKhieu|phenika|hutech|dhbkhn|bachkhoa|\.edu\.|^dh|rmit|dihoc|duhoc|anhngu|learn|study|teacher') THEN 'education_health'
        WHEN REGEXP_LIKE (sender, '^bv|benhvien|hospital|dakhoa') THEN 'education_health'
        ELSE 'other'
    END
"""

pattern = r"(?i)(?:then|else)\s+'([^']+)'"
lst_category = list(set(re.findall(pattern, brand_cat_expr)))

@status_check
def gen_brand_lxw_fts(spark, table_name, bt_table_name, bt_msisdn_column, out_dir, run_date, run_mode, common_config, level):
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
            SELECT msisdn, SENDER as sender, s3_file_date, DATETIME as record_time FROM {table_name}
            WHERE 1=1
                AND s3_file_date >= '{date_from_str}'
                AND s3_file_date < '{date_to_str}'
        """   

        if i==1:
            if level == "weekly":
                df_brand_raw = utils.spark_read_data_from_singlestore(spark, query)
                df_brand_raw = df_brand_raw.withColumn("sender", F.lower(F.trim(F.col("sender"))))\
                                        .withColumn("category", F.expr(brand_cat_expr)).persist()
                df_brand_raw.count()

                if run_mode == 'prod': 
                    df_brand_raw = df_brand_raw.where("left(msisdn, 4) = '0084' and length(msisdn) = 13")
     
                # Calculate total messages per user
                brand_fts = df_brand_raw.groupBy("msisdn").agg(
                    F.count("*").alias(f"brandname_sms_count_{freq_str}"),
                    *[
                        F.sum(F.when(F.col("category") == cat, 1).otherwise(0)).alias(f"brandname_sms_count_{cat}_{freq_str}")
                        for cat in lst_category
                    ]
                )
                
            else: 
                continue
        else: 
            if level != "weekly":
                lower_fts = utils.load_from_s3(spark, out_dir + "/l1w").where(f"date > {date_from_str} and date <= {date_to_str}")
                old_suffix = "l1w"
                new_suffix = freq_str

                # create sum count features
                exclude_list = ["msisdn", "date"]
                aggs = []
                fts_cols = [x for x in lower_fts.columns if x not in exclude_list]

                aggs.extend(
                    [F.sum(F.col(x)).alias(x.replace(old_suffix, new_suffix)) for x in fts_cols if any(sub in x for sub in ["_count_"])]
                )

                brand_fts = lower_fts.groupBy("msisdn").agg(*aggs)
            else: 
                continue

        # add pct for each category
        for cat in lst_category:
            brand_fts = brand_fts.withColumn(
                f"brandname_sms_pct_{cat}_{freq_str}",
                F.col(f"brandname_sms_count_{cat}_{freq_str}") / F.col(f"brandname_sms_count_{freq_str}")
            )

        # add pct between categories 
        for i in range(len(lst_category)):
            for j in range(i + 1, len(lst_category)):
                cat1 = lst_category[i]
                cat2 = lst_category[j]
                brand_fts = brand_fts.withColumn(
                    f"brandname_sms_pct_{cat1}_vs_{cat2}_{freq_str}",
                    F.col(f"brandname_sms_pct_{cat1}_{freq_str}") / F.col(f"brandname_sms_pct_{cat2}_{freq_str}")
                )
            
        # cal entropy feature 
        entropy_expr = " + ".join(
            [f"IFNULL(-brandname_sms_pct_{cat}_{freq_str} * LOG2(brandname_sms_pct_{cat}_{freq_str}), 0)" for cat in lst_category]
        )
        brand_fts = brand_fts.withColumn(
            f"brandname_entropy_{freq_str}",
            F.expr(entropy_expr)
        )

        # convert numerical to double type 
        brand_fts = brand_fts.select("msisdn", *[F.col(c).cast("double") for c in brand_fts.columns if c != "msisdn"])

        utils.save_to_s3(brand_fts, out_dir + f"/{freq_str}/date={run_date}")
        if i==1:
            if level == "weekly":
                df_brand_raw.unpersist()
                
@status_check
def merge_brand_fts(spark, out_dir, run_date,  **kwargs):
   
    print("merging lxw fts for", run_date)

    l1w = utils.load_from_s3(spark, out_dir + "/l1w").where(f"date='{run_date}'").drop("date")
    l4w = utils.load_from_s3(spark, out_dir + "/l4w").where(f"date='{run_date}'").drop("date")
    l12w = utils.load_from_s3(spark, out_dir + "/l12w").where(f"date='{run_date}'").drop("date")
    l24w = utils.load_from_s3(spark, out_dir + "/l24w").where(f"date='{run_date}'").drop("date")
    
    df_fts = l1w.join(l4w, on=["msisdn"], how='outer')\
            .join(l12w, on=["msisdn"], how='outer')\
            .join(l24w, on=["msisdn"], how='outer')

    # Create ratio features
    exclude_list = ["msisdn", "date"]

    fts_names = [col for col in l1w.columns if col not in exclude_list]
    fts_names = [x for x in fts_names if not any(sub in x for sub in ["pct"])]

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
    table_name = "blueinfo_vascdr_brandname_meta"

    run_create_feature(
        func_and_kwargs=(
            (gen_brand_lxw_fts, {'level':'weekly'}),
            (gen_brand_lxw_fts, {'level':'lxw'}),
            (merge_brand_fts, {})
        ),
        global_kwargs={
            'freq': 'weekly',
            'feature_name': 'brandname',
            'table_name': table_name,
        }
    )