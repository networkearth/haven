import awswrangler as wr
import logging 
import os
import yaml
import hashlib
import boto3

logger = logging.getLogger(__name__)

def configure(haven_dir):
    os.environ['HAVEN_DIR'] = haven_dir
    cwd = os.getcwd()

    os.chdir(haven_dir)
    os.environ['HAVEN_BRANCH'] = ''.join(os.popen('git branch --show-current').read().strip())
    os.chdir(cwd)

    with open(os.path.join(haven_dir, 'config.yaml'), 'r') as fh:
        config = yaml.safe_load(fh)

    os.environ['HAVEN_DATABASE'] = config['database']
    os.environ['HAVEN_NAME'] = f"{os.environ['HAVEN_BRANCH']}_{os.environ['HAVEN_DATABASE']}"


def create_database():
    haven_name = os.environ['HAVEN_NAME']
    databases = wr.catalog.databases()
    if haven_name not in databases.values:
        wr.catalog.create_database(haven_name)
    else:
        logger.warning(f'{haven_name} already exists, skipping creation...')


def build_path(table):
    haven_name = os.environ['HAVEN_NAME']
    return f's3://{haven_name}-database/{table}/'.replace('_', '-')

def get_bucket():
    haven_name = os.environ['HAVEN_NAME']
    return f'{haven_name}-database'.replace('_', '-')


def validate_against_schema(df, table, partition_cols):
    haven_name = os.environ['HAVEN_NAME']
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

def digest_partitions(s3_path):
    parts = s3_path.replace('s3://', '').split('/')[2:-1]
    return '/'.join(tuple(part.split('=')[-1] for part in parts))

def digest_s3_path(s3_path):
    parts = s3_path.replace('s3://', '').split('/')
    return parts[0], '/'.join(parts[1:])

def write_data(df, table, partition_cols):
    haven_name = os.environ['HAVEN_NAME']
    haven_dir = os.environ['HAVEN_DIR']
    
    if wr.catalog.does_table_exist(database=haven_name, table=table):
        validate_against_schema(df, table, partition_cols)
    else:
        column_types, partition_types = wr.catalog.extract_athena_types(df, partition_cols=partition_cols)
        schema = {
            'columns': column_types,
            'partition_columns': partition_types,
            'partition_order': partition_cols
        }
        with open(os.path.join(haven_dir, 'schemas', f'{table}.yaml'), 'w') as fh:
            yaml.dump(schema, fh, default_flow_style=False)

    s3_paths = wr.s3.to_parquet(
        df=df,
        path=build_path(table),
        dataset=True,
        database=haven_name,
        table=table,
        mode="overwrite_partitions",
        partition_cols=partition_cols,
    )['paths']

    manifest_path = os.path.join(haven_dir, 'manifests', f'{table}.yaml')
    if os.path.exists(manifest_path):
        with open(manifest_path, 'r') as fh:
            manifests = yaml.safe_load(fh)
    else:
        manifests = {}

    s3_client = boto3.client('s3')

    for path in s3_paths:
        bucket, key = digest_s3_path(path)
        data_bytes = s3_client.get_object(Bucket=bucket, Key=key)['Body'].read()
        sha256_hash = hashlib.sha256(data_bytes).hexdigest()
        partitions = digest_partitions(path)
        manifests[partitions] = sha256_hash

    with open(manifest_path, 'w') as fh:
        yaml.dump(manifests, fh, default_flow_style=False)


def get_partition_order(table):
    haven_dir = os.environ['HAVEN_DIR']
    schema_path = os.path.join(haven_dir, 'schemas', f'{table}.yaml')
    with open(schema_path, 'r') as fh:
        schema = yaml.safe_load(fh)
    return schema['partition_order']

def delete_data(table, partitions):
    partition_cols = get_partition_order(table)
    paths = [
        '/'.join([build_path(table)[:-1]] + [f'{col}={partition[col]}' for col in partition_cols]) + '/'
        for partition in partitions
    ]
    for path in paths:
        wr.s3.delete_objects(path=path)

    haven_dir = os.environ['HAVEN_DIR']
    manifest_path = os.path.join(haven_dir, 'manifests', f'{table}.yaml')
    with open(manifest_path, 'r') as fh:
        manifests = yaml.safe_load(fh)

    keys = ['/'.join([partition[col] for col in partition_cols]) for partition in partitions]
    for key in keys:
        del manifests[key]
    
    with open(manifest_path, 'w') as fh:
        yaml.dump(manifests, fh, default_flow_style=False)

    
def drop_table(table):
    wr.s3.delete_objects(path=build_path(table))

    haven_name = os.environ['HAVEN_NAME']
    wr.catalog.delete_table_if_exists(database=haven_name, table=table)

    haven_dir = os.environ['HAVEN_DIR']
    schema_path = os.path.join(haven_dir, 'schemas', f'{table}.yaml')
    if os.path.exists(schema_path):
        os.remove(schema_path)

    manifest_path = os.path.join(haven_dir, 'manifests', f'{table}.yaml')
    if os.path.exists(manifest_path):
        os.remove(manifest_path)


def drop_database():
    haven_name = os.environ['HAVEN_NAME']
    for _ in wr.catalog.get_tables(database=haven_name):
        raise ValueError(f'{haven_name} is not empty, cannot drop database...')
    wr.catalog.delete_database(haven_name)


def read_data(query):
    haven_name = os.environ['HAVEN_NAME']
    return wr.athena.read_sql_query(query, database=haven_name)
