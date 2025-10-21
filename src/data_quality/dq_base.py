"""
This module defines the abstract base class `BaseDQCheck` for implementing data quality (DQ) checks in a Spark-based data pipeline. It provides a standardized interface and workflow for running various DQ dimensions (such as completeness, uniqueness, etc.) on data tables, supporting both database and S3 sources.

Classes:
    BaseDQCheck (ABC): Abstract base class for DQ checks. Subclasses should define specific DQ dimensions and metrics.

Key Features:
    - Manages Spark session and DQ dimension/metric configuration.
    - Loads data from either a database or S3, creating temporary views as needed.
    - Iterates through configured metrics, applying rules and severity levels.
    - Aggregates results from all checks into a single pandas DataFrame.
    - Cleans up temporary views after processing.

Usage:
    Subclass `BaseDQCheck` to implement specific DQ checks by defining the `dimension` and `metrics` attributes, and providing check methods for each metric.
"""

from abc import ABC
import pandas as pd
from pyspark.sql import SparkSession
from src.utils import common as utils


from abc import ABC
from pyspark.sql import SparkSession
import pandas as pd

class BaseDQCheck(ABC):
    """
    Abstract base class for data quality checks.

    Attributes:
        spark (SparkSession): The active Spark session.
        dimension (str): Name of the DQ dimension (e.g., "completeness").
        metrics (list): List of tuples (metric_name, check_method) for the dimension.
    """

    def __init__(self, spark: SparkSession):
        """
        Initializes the base data quality checker.

        Args:
            spark (SparkSession): Spark session to use for data operations.
        """
        self.spark = spark
        self.dimension = None
        self.metrics = []  # List of (metric_name, check_method)

    def run(self, spark, table: str, destination: str, source: str, snapshot: str, layer: str,
            rules: dict, common_config, custom_metric_filter=None):
        """
        Executes all configured data quality checks for this dimension.

        Args:
            spark (SparkSession): The Spark session.
            table (str): Table name to check.
            destination (str): Destination path or database name.
            source (str): Either 'database' or another source like 's3'.
            snapshot (str): Timing information.
            layer (str): Data layer (e.g., feature, raw).
            rules (dict): Dictionary of rules for all dimensions.
            common_config (dict): Shared config across all checks.
            custom_metric_filter (callable, optional): Function to filter metrics dynamically.

        Returns:
            pd.DataFrame: Combined DataFrame of all check results.
        """
        results = []

        # Determine data source and prepare table/view
        if source == 'database':
            table_name = table  # Use table name directly for database source
        else:
            # Load DataFrame from S3 and create a temporary view for Spark SQL
            df = utils.load_from_s3(spark, destination + snapshot)
            table_name = table + '_view'
            df.createOrReplaceTempView(table_name)

        # Retrieve rules specific to this DQ dimension
        dim_rules = rules.get(self.dimension, {})
        for metric_name, check_method in self.metrics:
            # Optionally filter metrics using a custom filter function
            if custom_metric_filter and not custom_metric_filter(metric_name, layer):
                continue

            # Only process metrics that have rules defined
            if metric_name in dim_rules:
                # Get severity for the metric, defaulting to 'low' if not specified
                severity = dim_rules.get(f'active_{metric_name}', 'low')
                
            # Run the check method for this metric and collect the result
            sub_df = check_method(
                spark, table_name, destination, snapshot, layer,
                dim_rules[metric_name], severity,
                metric_name, source, common_config
            )
            if sub_df is None: 
                continue
            results.append(sub_df)

        # Clean up temporary view if it was created
        if '_view' in table_name:
            spark.catalog.dropTempView(table_name)

        # Concatenate all results into a single DataFrame, or return empty DataFrame if no results
        return pd.concat(results, axis=0, ignore_index=True) if results else pd.DataFrame()
