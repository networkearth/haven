import awswrangler as wr
import logging 

logger = logging.getLogger(__name__)


def create_database(branch, database):
    haven_name = f'{branch}_{database}'
    databases = wr.catalog.databases()
    if haven_name not in databases.values:
        wr.catalog.create_database(haven_name)
    else:
        logger.warning(f'{haven_name} already exists, skipping creation...')


def build_path(branch, database, table):
    haven_name = f'{branch}_{database}'
    return f's3://{haven_name}-database/{table}/'.replace('_', '-')


def validate_against_schema(df, branch, database, table, partition_cols):
    haven_name = f'{branch}_{database}'
    glue_schema = wr.catalog.table(database=haven_name, table=table)
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


def write_data(df, branch, database, table, partition_cols):
    haven_name = f'{branch}_{database}'
    
    if wr.catalog.does_table_exist(database=haven_name, table=table):
        validate_against_schema(df, branch, database, table, partition_cols)

    wr.s3.to_parquet(
        df=df,
        path=build_path(branch, database, table),
        dataset=True,
        database=haven_name,
        table=table,
        mode="overwrite_partitions",
        partition_cols=partition_cols,
    )


def drop_table(branch, database, table):
    wr.s3.delete_objects(path=build_path(branch, database, table))

    haven_name = f'{branch}_{database}'
    wr.catalog.delete_table_if_exists(database=haven_name, table=table)


def drop_database(branch, database):
    haven_name = f'{branch}_{database}'
    for _ in wr.catalog.get_tables(database=haven_name):
        raise ValueError(f'{haven_name} is not empty, cannot drop database...')
    wr.catalog.delete_database(haven_name)


def read_data(branch, database, query):
    haven_name = f'{branch}_{database}'
    return wr.athena.read_sql_query(query, database=haven_name)
