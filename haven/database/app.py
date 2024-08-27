from aws_cdk import App, Environment, Stack

from constructs import Construct


class DatabaseStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)


def database_app(branch: str):
    app = App()
    config = app.node.try_get_context('config')
    environment = Environment(account=config['account'], region=config['region'])

    DatabaseStack(app, branch + '-' + config['database'], env=environment)

    app.synth()