from aws_cdk import (
    App, 
    Environment, 
    Stack,
    aws_s3 as s3
)

from constructs import Construct


class BackupStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        bucket_name = id
        bucket = s3.Bucket(
            self, bucket_name, bucket_name=bucket_name,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
        )


def backup_app():
    app = App()
    config = app.node.try_get_context('config')
    environment = Environment(account=config['account'], region=config['region'])

    stack_name = (config['database'] + '_backup').replace('_', '-')
    stack = BackupStack(app, stack_name, env=environment)

    app.synth()