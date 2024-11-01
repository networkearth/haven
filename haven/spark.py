"""
Functions for interacting with your Haven database with spark.
"""

import os
import awswrangler as wr

from .db import build_path, validate_against_schema

# https://medium.com/@afsalopes/how-query-aws-athena-with-pyspark-894b667ba335

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
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") # prevents writing _SUCCESS files
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.amazonaws.com")
        .config("spark.jars", "s3a://mirrorverse-emr/jars/AthenaJDBC42-2.0.33.jar")
        .config("spark.driver.extraClassPath", "s3a://mirrorverse-emr/jars/AthenaJDBC42-2.0.33.jar")
        .config("spark.executor.extraClassPath", "s3a://mirrorverse-emr/jars/AthenaJDBC42-2.0.33.jar")
        #.config("spark.jars","https://s3.amazonaws.com/athena-downloads/drivers/JDBC/SimbaAthenaJDBC-2.0.33.1003/AthenaJDBC42-2.0.33.jar")
        .config("spark.sql.sources.partitionOverwriteMode", "DYNAMIC") # overwrite only the partitions that have changed
    )

def read_data(sql, spark, query_results_bucket, region=""):
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
    """
    region = region or os.environ["AWS_REGION"]

    return (
        spark.read.format("jdbc")
            .option("driver", "com.simba.athena.jdbc.Driver")
            .option("url", f"jdbc:awsathena://athena.{region}.amazonaws.com:443")
            .option("AwsCredentialsProviderClass", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
            .option("S3OutputLocation", query_results_bucket)
            .option("query", sql)
            .load()
    )

def write_data(df, table, partition_cols, database=""):
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

    # register the partitions
    wr.athena.repair_table(
        table=table,
        database=database
    )
