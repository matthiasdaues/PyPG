import os                                           # noqa: F401

from sqlalchemy import create_engine                # noqa: F401
from sqlalchemy import text, quoted_name            # noqa: F401
from sqlalchemy.exc import SQLAlchemyError          # noqa: F401

import  PyPG.code.utils.db_connect as db_connect
from PyPG.code.utils.write_to_log import write_to_log
from PyPG.code.utils.write_to_setup_statements import write_to_setup_statements
from PyPG.code.utils.write_to_undo_statements import write_to_undo_statements
from PyPG.code.utils.execute_statement import execute_statement
from PyPG.code.core.read_configuration import read_configuration


def create_extensions(config):
    """
    Gets database configuration parameters from the given
    "config.yml" file and deploys the database specific
    configurations for extensions.
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

    # Get list of installed extensions and check for existing
    # installation. Install when necessary.
    get_extensions = text("select extname from pg_catalog.pg_extension;")

    with engine.connect() as conn:
        result = conn.execute(get_extensions).fetchall()
        installed_extensions = []

        for i in result:
            installed_extensions.append(i[0])

    for i in configuration['extensions']:
        extension = i
        create_extension = text(f"create extension if not exists {quoted_name(extension, False)} cascade;")
        drop_extension = text(f"drop extension if exists {quoted_name(extension, False)} cascade;")
        if extension in installed_extensions:
            message = text(f"INFO: Extension {extension} exists.")
            write_to_log(log, message)
            write_to_setup_statements(setup_statements, create_extension)
            write_to_undo_statements(undo_statements, drop_extension)
        else:
            statement = create_extension
            success = f"INFO: Extension {extension} has been installed."
            error = f"ERROR: Extension {extension} couldn't be installed"
            message = execute_statement(engine, statement, success, error)
            
            write_to_log(log, message)
            write_to_setup_statements(setup_statements, create_extension)
            write_to_undo_statements(undo_statements, drop_extension)
