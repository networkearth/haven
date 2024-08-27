from aws_cdk import (
    App, 
    Environment, 
    Stack, 
    RemovalPolicy,
    aws_s3 as s3
)

from constructs import Construct


class DatabaseStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        bucket_name = f'{id}-bucket'
        bucket = s3.Bucket(
            self, bucket_name, bucket_name=bucket_name,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
        )


def database_app(branch: str):
    app = App()
    config = app.node.try_get_context('config')
    environment = Environment(account=config['account'], region=config['region'])

    stack = DatabaseStack(app, branch + '-' + config['database'], env=environment)

    app.synth()