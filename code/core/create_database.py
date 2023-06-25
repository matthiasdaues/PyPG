import yaml
import os

import sqlalchemy_utils as sal_utils                # noqa: F401
from sqlalchemy import create_engine                # noqa: F401

import utils.db_connect as db_connect
from core.read_configuration import read_configuration


def create_database(config, connection):
    """
    Gets database configuration parameters from the given
    "config.yml" file and creates the database as specified in
    the db_name key.
    """
   
    # read the configuration
    configuration = read_configuration(config)

    # define db_name and define directory path as absolute path
    db_name = configuration['db_name']
    path = configuration['paths']['db_details_path']

    # PostgreSQL connection information
    conn_string = db_connect.get_db_connection(config, connection)

    # Create the SQLAlchemy engine
    engine = create_engine(conn_string)

    # create the database
    if not sal_utils.database_exists(engine.url): 
        sal_utils.create_database(engine.url)
        print(f"INFO: Database {db_name} created.")
        create_db_file = os.path.join(path, 'create_db.sql')
        with open(create_db_file, "w") as create_db_f:
            create_db_f.write(f"create database {db_name};")
        drop_db_file = os.path.join(path, 'drop_db.sql')
        with open(drop_db_file, "w") as drop_db_f:
            drop_db_f.write(f"drop database {db_name};")
        print("INFO: Create and drop database file created.")
    else:
        print(f"INFO: Database {db_name} already exists.")
