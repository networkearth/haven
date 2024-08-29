import os
import click

from haven.database.app import database_app 
from haven.backup.app import backup_app
from haven.control import determine_new_data, backup_new_data


@click.group()
def cli():
    pass

@cli.command()
def setup():
    print("Setting Up Haven")

@cli.command()
def teardown():
    print("Tearing down Haven")

@cli.command()
@click.argument('path')
def add(path):
    os.system(f'git add {path}')

@cli.command()
@click.option('--message', '-m', required=True)
def commit(message):
    new_data = determine_new_data()
    backup_new_data(new_data)
    os.system(f'git commit -m "{message}"')


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