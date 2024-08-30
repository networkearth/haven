import os
import awswrangler as wr


def create_database(database):
    databases = wr.catalog.databases()
    assert database not in databases.values, f'{database} already exists...'

    wr.catalog.create_database(database)

def build_path(table, database=None):
    database = database or os.environ['HAVEN_DATABASE']
    return f's3://{database}-database/{table}/'.replace('_', '-')

def get_bucket():
    haven_name = os.environ['HAVEN_NAME']
    return f'{haven_name}-database'.replace('_', '-')

def validate_against_schema(df, table, partition_cols, database=None):
    database = database or os.environ['HAVEN_DATABASE']

    glue_schema = wr.catalog.table(database=database, table=table)
    expected_partition_types = {}
    expected_column_types = {}
    for _, row in glue_schema.iterrows():
        if row['Partition'] == True:
            expected_partition_types[row['Column Name']] = row['Type']
        else:
            expected_column_types[row['Column Name']] = row['Type']

    column_types, partition_types = wr.catalog.extract_athena_types(df, partition_cols=partition_cols)

    assert column_types == expected_column_types, f'Data columns do not match schema: {column_types} != {expected_column_types}'
    assert partition_types == expected_partition_types, f'Partition columns do not match schema: {partition_types} != {expected_partition_types}'

def write_data(df, table, partition_cols, database=None):
    database = database or os.environ['HAVEN_DATABASE']
    
    if wr.catalog.does_table_exist(database=database, table=table):
        validate_against_schema(df, table, partition_cols, database)

    wr.s3.to_parquet(
        df=df,
        path=build_path(table, database),
        dataset=True,
        database=database,
        table=table,
        mode="overwrite_partitions",
        partition_cols=partition_cols,
    )

def delete_data(table, partitions, database=None):
    database = database or os.environ['HAVEN_DATABASE']

    partition_cols = []
    glue_schema = wr.catalog.table(database=database, table=table)
    for _, row in glue_schema.iterrows():
        if row['Partition'] == True:
            partition_cols.append(row['Column Name'])
    paths = [
        '/'.join([build_path(table, database)[:-1]] + [f'{col}={partition[col]}' for col in partition_cols]) + '/'
        for partition in partitions
    ]
    for path in paths:
        wr.s3.delete_objects(path=path)
    
def drop_table(table, database=None):
    database = database or os.environ['HAVEN_DATABASE']

    wr.s3.delete_objects(path=build_path(table, database))
    wr.catalog.delete_table_if_exists(database=database, table=table)


def drop_database(database):
    for _ in wr.catalog.get_tables(database=database):
        raise ValueError(f'{database} is not empty, cannot drop database...')
    wr.catalog.delete_database(database)


def read_data(query, database=None):
    database = database or os.environ['HAVEN_DATABASE']

    return wr.athena.read_sql_query(query, database=database)
