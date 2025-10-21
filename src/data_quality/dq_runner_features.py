
"""
This script is the main entry point for running data quality (DQ) checks across multiple tables and dimensions.
It orchestrates the execution of all DQ dimensions (completeness, consistency, timeliness, uniqueness, validity)
for a given set of tables and a date range, and saves the results to a log.

Functions:
    - run_dq: Main function to run DQ checks for all tables and dimensions.

Usage:
    Run as a script or import and call run_dq() in a pipeline.
"""

import sys
from src.utils import common as utils, data_quality_utils as dq_utils 
from src.data_quality.checks import (completeness_check, 
                                 consistency_check,
                                 timeliness_check,
                                 uniqueness_check,
                                 validity_check)

import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import col

# Mapping of DQ dimension names to their corresponding checker classes.
# Each checker class implements the logic for a specific data quality dimension.
checker_classes = {
    'completeness': completeness_check.CompletenessChecker,
    'consistency': consistency_check.ConsistencyChecker,
    'timeliness': timeliness_check.TimelinessChecker,
    'uniqueness': uniqueness_check.UniquenessChecker,
    # 'validity': validity_check.ValidityChecker
}


def run_dq(common_config, dct_tables, layer='feature'):
    """
    Run data quality checks for all tables and dimensions for a given date range and layer.

    Args:
        common_config (dict): Common configuration loaded from YAML.
        dct_tables (dict): Dictionary of tables to check (table name -> destination).
        layer (str): Data layer to check ('feature' or 'database').
    """

    spark = utils.create_spark_instance()
    if layer == 'database': 
        rules = utils.load_config("configs", "dq_config_raw.yaml")
    else: 
        rules = utils.load_config("configs", "dq_config_feature.yaml")

    # Parse command-line arguments for date range
    args = utils.process_args_to_dict(sys.argv[1])
    dt_to = datetime.strptime(args['dt_to'], "%Y%m%d")

    source = 's3' if layer == 'feature' else 'database'
    all_result = []
    for table, info in dct_tables.items():
        destination = info['destination']
        dt_from = datetime.strptime(args['dt_from'], "%Y%m%d") + relativedelta(days=1)

        while dt_from <= dt_to:
            run_date = dt_from.strftime('%Y%m%d')

            # Check if run_date is Monday if layer is is 'feature'
            if layer == 'feature' and dt_from.weekday() != 0: 
                print(f"Skipping run: Only run on Mondays for feature layer. Skipped date: {run_date}")
                dt_from = dt_from + relativedelta(days=1)
                continue

            # Run all DQ dimensions for the table and date
            for dimension in rules.keys():
                if dimension == 'uniqueness' and table == 'device':
                    continue
                
                checker_class = checker_classes.get(dimension)
                if checker_class:
                    # msg = f"RUNNING NODE: {table} | DATE: {run_date} | DIMENSION: {dimension}"
                    # utils.send_msg_to_telegram(msg)
                    checker = checker_class(spark)
                    result = checker.run(spark, table, destination, source, run_date, layer, rules, common_config)
                    all_result.append(result)
            dt_from = dt_from + relativedelta(days=1)

    if all_result == []: 
        return
        
    # Save quality status to S3/log
    combined_pandas_df = pd.concat(all_result, axis=0, ignore_index=True)
    combined_pandas_df['metric_value'] = combined_pandas_df['metric_value'].astype(str)

    df_result = spark.createDataFrame(combined_pandas_df)
    dq_utils.update_log_data(spark, df_result, common_config['outdir']['logs'] + 'log_data_quality')

    # Convert all columns to string type
    for c in df_result.columns: 
        df_result = df_result.withColumn(c, col(c).cast("string"))
    dq_utils.trace_event(df_result, 'feature')

if __name__ == '__main__':
    # Example main entry point for running DQ checks
    layer_lv = 'feature'
    common_config = utils.load_config("configs", "common.yaml")
    dct_tables = common_config['feature_destination'] if layer_lv == 'feature' else common_config['raw_destination']
    run_dq(common_config, dct_tables, layer=layer_lv)