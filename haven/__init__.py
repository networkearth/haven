import os
import click

from haven.database.app import database_app 
from haven.backup.app import backup_app


@click.group()
def cli():
    pass

@cli.command()
def setup():
    print("Setting Up Haven")

@cli.command()
def teardown():
    print("Tearing down Haven")

@click.group()
def build_cli():
    pass

@build_cli.command()
def database():
    branch = ''.join(os.popen('git branch --show-current').read().strip())
    database_app(branch)

@build_cli.command()
def backup():
    backup_app()