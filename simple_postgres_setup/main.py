import os
from dotenv import load_dotenv
import simple_postgres_setup.code.core as core

load_dotenv(override=True)

def setup_database(config: str):
    """
    setup the database as specified in
    the config.yml using the parameters
    provided in the .env file.
    """
    # create the local folders for documentation and
    # logging. Create the temporary password store.
    core.create_local_folders_and_files(config)

    # create database on cluster as specified in the connection.yml
    core.create_database(config)

    # create extensions in the database.
    core.create_extensions(config)

    # create the users (login roles) if they don't already
    # exist on the pg-cluster.
    core.create_users(config)

    # create the schemas in the database along with their
    # respective access tiers.
    core.create_schemas(config)

    # grant the respective privileges per user on the schemas
    # as defined in the three access tiers.
    core.create_policies(config)

    # TODO: Create a function to collect all roles granted to postgres in the process and revoke them.
    # TODO: Create sql files with grant and revoke statements in the database folder structure
    # TODO: Create logging


def drop_database(config: str):
    """
    drop the database as specified in
    the config.yml using the parameters
    provided in the .env file.
    """
    config = './inputs/config.yml'

    # drop database, users and roles
    core.drop_database(config)
    core.drop_users(config)
    core.drop_roles(config)
