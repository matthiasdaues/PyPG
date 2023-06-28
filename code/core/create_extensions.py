import os                                           # noqa: F401

from sqlalchemy import create_engine                # noqa: F401
from sqlalchemy import text, quoted_name            # noqa: F401
from sqlalchemy.exc import SQLAlchemyError          # noqa: F401

import utils.db_connect as db_connect
from utils.execute_statement import execute_statement
from core.read_configuration import read_configuration


def create_extensions(config, connection):
    """
    Gets database configuration parameters from the given
    "config.yml" file and deploys the database specific
    configurations for extensions.
    """

    # read the configuration
    configuration = read_configuration(config)

    # define db_name and define directory path as absolute path
    # path = configuration['paths']['extensions_path']

    # PostgreSQL connection information
    conn_string = db_connect.get_db_connection(config, connection)

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
        if extension in installed_extensions:
            print(f"INFO: Extension {extension} exists.")
        else:

            statement = create_extension
            success = f"INFO: Extension {extension} has been installed."
            error = f"ERROR: Extension {extension} couldn't be installed"

            log = execute_statement(engine, statement, success, error)
            print(log)

            # with engine.connect() as conn:
            #     transaction = conn.begin()
            #     try:
            #         conn.execute(create_extension)
            #         transaction.commit()
            #         print(f"INFO: Extension {extension} has been installed.")
            #     except SQLAlchemyError as e:
            #         transaction.rollback()
            #         print(f"ERROR: Extension {extension} couldn't be installed: {e}.")
            #     continue
                

    # with engine.connect() as conn:
    #     result = conn.execute(get_extensions).fetchall()
    #     installed_extensions = []

    # for i in result:
    #     installed_extensions.append(i[0])
