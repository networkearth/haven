"""
Haven CLI
"""

import os
import json
import click
import yaml

from haven.db import create_database


def setup_stack(config):
    """
    :param config: The configuration for the stack.
    :type config: dict
    """
    # start by creating the database directory
    database = config["database"]
    if os.path.exists(database):
        raise ValueError(
            f"A directory '{database}' already exists, please remove it first..."
        )
    os.mkdir(database)

    # next add the stack file
    builder_dir = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(builder_dir, "app.py"), "r") as f:
        app = f.read()

    cwd = os.getcwd()
    os.chdir(database)
    with open("app.py", "w") as f:
        f.write(app)

    # now for the cdk.json file
    cdk_config = {"app": "python app.py", "context": {"config": config}}
    with open("cdk.json", "w") as f:
        json.dump(cdk_config, f, indent=4, sort_keys=True)

    # back up to the original directory
    os.chdir(cwd)


# pylint: disable=missing-function-docstring
@click.group()
def cli():
    pass


@cli.command()
@click.argument("config_path")
def init(config_path):
    """
    :param config_path: The path to the configuration file.
    :type config_path: str
    """
    # read config
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # setup the stack
    setup_stack(config)

    # deploy the stack
    cwd = os.getcwd()
    os.chdir(config["database"])
    os.system("cdk deploy")
    os.chdir(cwd)

    # now setup the database in glue
    create_database(config["database"])
