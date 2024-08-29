import os
from collections import defaultdict
import pygit2
import boto3
import yaml

def determine_new_data():
    repo = pygit2.Repository(os.getcwd())
    prior_commit = repo.revparse_single('HEAD')
    diff = repo.diff(prior_commit, None, cached=True)

    new_data = defaultdict(dict)
    for patch in diff:
        patch_file = patch.delta.new_file.path
        if '/' not in patch_file or patch_file.split('/')[-2] != 'manifests':
            continue
        table = patch_file.split('/')[-1].split('.')[0]
        for hunk in patch.hunks:
            for line in hunk.lines:
                if line.origin == '+':
                    keys, _hash = line.content.split(':')
                    new_data[table][keys.strip()] = _hash.strip()

    return new_data

from haven.db import configure, get_bucket

def backup_new_data(new_data):
    s3_client = boto3.client('s3')

    configure(os.getcwd())

    for table, data in new_data.items():
        
        with open(f'{os.environ["HAVEN_DIR"]}/schemas/{table}.yaml', 'r') as fh:
            schema = yaml.safe_load(fh)
        partition_cols = schema['partition_order']
        for keys, _hash in data.items():
            source_bucket = get_bucket()
            prefix = '/'.join(
                [table] + ['='.join([col, key]) for col, key in zip(partition_cols, keys.split(','))]
            ) + '/'
            response = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=prefix)
            assert len(response['Contents']) == 1, f"Expected 1 object, got {len(response['Contents'])}"
            source_key = response['Contents'][0]['Key']

            destination_bucket = (os.environ['HAVEN_DATABASE'] + '-backup').replace('_', '-')
            destination_key = f'{_hash}.snappy.parquet'

            print(
                f'Copying s3://{source_bucket}/{source_key} to s3://{destination_bucket}/{destination_key}'
            )
            copy_source = {
                'Bucket': source_bucket,
                'Key': source_key
            }

            s3_client.copy(copy_source, destination_bucket, destination_key)
