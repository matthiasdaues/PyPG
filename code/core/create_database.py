import sqlalchemy_utils as sal_utils                # noqa: F401
from sqlalchemy import create_engine                # noqa: F401

import code.utils.db_connect as db_connect
from code.utils.write_to_log import write_to_log
from code.utils.write_to_setup_statements import write_to_setup_statements
from code.utils.write_to_undo_statements import write_to_undo_statements
from code.core.read_configuration import read_configuration


def create_database(config):
    """
    Gets database configuration parameters from the given
    "config.yml" file and creates the database as specified in
    the db_name key.
    """

    # read the configuration
    configuration = read_configuration(config)

    # define db_name and define directory path as absolute path
    db_name = configuration['db_name']
    setup_statements = configuration['files']['setup_statements']
    undo_statements = configuration['files']['undo_statements']
    log = configuration['files']['log']

    # PostgreSQL connection information
    conn_string = db_connect.get_db_connection(config)

    # Create the SQLAlchemy engine
    engine = create_engine(conn_string)

    # create the database create and drop statements as
    # first entries in the setup and undo statement collections
    setup_statement = f"create database {db_name};"
    undo_statement = f"drop database {db_name};"
    with open(setup_statements, "w"):
        write_to_setup_statements(setup_statements, setup_statement)
    with open(undo_statements, "w"):
        write_to_undo_statements(undo_statements, undo_statement)

    # create the database
    if not sal_utils.database_exists(engine.url):
        sal_utils.create_database(engine.url)
        message = f"INFO: Database {db_name} created."
        write_to_log(log, message)
    else:
        message = f"INFO: Database {db_name} already exists."
        write_to_log(log, message)
