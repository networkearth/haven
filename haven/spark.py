"""
Functions for interacting with your Haven database with spark.
"""

import os
import awswrangler as wr

from .db import build_path, validate_against_schema

# https://medium.com/@afsalopes/how-query-aws-athena-with-pyspark-894b667ba335
# https://repost.aws/questions/QUV7zwvwFJTlqi1PqX8hmZFg/classnotfoundexception-emrfilesystem-error-while-setting-spark-driver-class-path-config-in-emr-serverless

# pylint: disable=line-too-long
def configure(spark_session, region="", hadoop_version="3.3.4"):
    """
    :param spark_session: The SparkSession to configure.
    :type spark_session: pyspark.sql.SparkSession
    :param region: The AWS region. If not provided, will use the value of the 
        AWS_REGION environment variable.
    :type region: str
    :param hadoop_version: The version of Hadoop to match. Default is 3.3.4.
    :type hadoop_version: str
    :return: The configured SparkSession.
    :rtype: pyspark.sql.SparkSession

    Should be called in something like the following way:

    from pyspark.sql import SparkSession

    spark_session = SparkSession.builder
    spark_session = configure(spark_session)
    spark = spark_session.config(...).getOrCreate()

    To find your hadoop version, run the following command in your pyspark shell:
    sc._gateway.jvm.org.apache.hadoop.util.VersionInfo.getVersion()
    """
    region = region or os.environ["AWS_REGION"]

    return (
        spark_session
        .config("spark.jars.packages", f"org.apache.hadoop:hadoop-aws:{hadoop_version}")
        # prevents writing _SUCCESS files
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        # this line only effects local development. For EMR watercycle adds
        # this to the submit config
        .config("spark.jars", "https://s3.amazonaws.com/athena-downloads/drivers/JDBC/SimbaAthenaJDBC-2.0.33.1003/AthenaJDBC42-2.0.33.jar")
        # overwrite only the partitions that have changed
        .config("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")
    )

def read_data(sql, spark, query_results_bucket, region="", public_internet_access=False):
    """
    :param sql: The SQL query to run.
    :type sql: str
    :param spark: The SparkSession to use.
    :type spark: pyspark.sql.SparkSession
    :param query_results_bucket: The S3 bucket to store the query results.
    :type query_results_bucket: str
    :param region: The AWS region. If not provided, will use the value of the
        AWS_REGION environment variable.
    :type region: str
    :return: The DataFrame resulting from the query.
    :rtype: pyspark.sql.DataFrame
    :param public_internet_access: Whether public internet access is enabled 
        in the EMR cluster's VPC. Default is False.
    :type public_internet_access: bool
    """
    # if public internet access is unavailable the load function
    # will fail with a timeout error but EMR will not raise an error
    # itself and just hang indefinitely
    assert public_internet_access, "read_data requires public_internet_access=True"

    region = region or os.environ["AWS_REGION"]

    return (
        spark.read.format("jdbc")
            .option("driver", "com.simba.athena.jdbc.Driver")
            .option("url", f"jdbc:awsathena://athena.{region}.amazonaws.com:443")
            .option("AwsCredentialsProviderClass", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
            .option("S3OutputLocation", query_results_bucket)
            .option("query", sql)
            .load()
    )

def write_partitions(df, table, partition_cols, region="", database=""):
    """
    :param df: The DataFrame to write.
    :type df: pyspark.sql.DataFrame
    :param table: The name of the table.
    :type table: str
    :param partition_cols: The partition columns.
    :type partition_cols: list
    :param database: The name of the database. If not provided,
        will use the value of the HAVEN_DATABASE environment variable.
    :type database: str
    """
    database = database or os.environ["HAVEN_DATABASE"]
    region = region or os.environ["AWS_REGION"]
    # this environment variable is required by boto3
    # while on EMR
    os.environ["AWS_DEFAULT_REGION"] = region

    # the following works even if public internet access is disabled
    if wr.catalog.does_table_exist(database=database, table=table):
        validate_against_schema(df, table, partition_cols, database, spark=True)
    else:
        dtypes = dict(df.dtypes)
        wr.catalog.create_parquet_table(
            database=database,
            table=table,
            path=build_path(table, database),
            compression='snappy',
            columns_types={col: _type for col, _type in dtypes.items() if col not in partition_cols},
            partitions_types={col: dtypes[col] for col in partition_cols}
        )

    path = build_path(table, database).replace('s3', 's3a')

    df.write.mode("overwrite").partitionBy(*partition_cols).parquet(path)


def register_partitions(table, database="", public_internet_access=False):
    """
    :param table: The name of the table.
    :type table: str
    :param database: The name of the database. If not provided,
        will use the value of the HAVEN_DATABASE environment variable.
    :type database: str
    :param public_internet_access: Whether public internet access is enabled 
        in the EMR cluster's VPC. Default is False.
    :type public_internet_access: bool
    """
    # if public internet access is unavailable the aws wrangler function
    # will fail with a timeout error but EMR will not raise an error
    # itself and just hang indefinitely
    assert public_internet_access, "register_partitions requires public_internet_access=True"

    database = database or os.environ["HAVEN_DATABASE"]
    wr.athena.repair_table(
        table=table,
        database=database
    )
