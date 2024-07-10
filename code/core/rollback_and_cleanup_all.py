import os

import sqlalchemy_utils as sal_utils                # noqa: F401
from sqlalchemy import create_engine                # noqa: F401
from sqlalchemy import text, quoted_name            # noqa: F401
from sqlalchemy.exc import SQLAlchemyError          # noqa: F401

import utils.db_connect as db_connect
from core.read_configuration import read_configuration
from core.create_schemas import create_schema_roles


# Password for authorizing the drop operation via user input
password = 'drop_it_now'


def drop_database(config, connection):
    """
    Gets database configuration parameters from the given
    "config.yml" file and drops the database as specified in
    the db_name key if it exists.
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

   # Authorize with a confirmation prompt
    user_confirm = input("Enter Y to proceed: ")

    # Compare the user's input with the validation rule
    if user_confirm == 'Y':
        print("Access granted. Database will be dropped.")

        # If the password matches, drop the database:
        if sal_utils.database_exists(engine.url): 
            sal_utils.drop_database(engine.url)
            print(f"INFO: Database {db_name} dropped.")
        else:
            print(f"INFO: Database {db_name} does not exist.")
    
    else:
        print("Access denied. Incorrect password.")


def drop_users(config, connection):
    """
    Gets database configuration parameters from the given
    "config.yml" file and drops existing users from the list
    specified in the users key.
    """
   
    # read the configuration
    configuration = read_configuration(config)

    # PostgreSQL connection information
    conn_string = db_connect.get_cluster_connection(connection)

    # Create the SQLAlchemy engine
    engine = create_engine(conn_string)

   # Authorize with a confirmation prompt
    user_confirm = input("Enter Y to proceed: ")

    # Compare the user's input with the validation rule
    if user_confirm == 'Y':
        print("Access granted. Users will be dropped.")

        # If the password matches, drop the users:
        for user in configuration['users']:
  
            # Prepare the drop statement
            drop_user = text(f"drop user {quoted_name(user, False)};")

            # Drop the user on the cluster
            with engine.connect() as conn:
                transaction = conn.begin()
                try:
                    conn.execute(drop_user)
                    transaction.commit()
                    print(f"INFO: User {user} has been dropped.")
                except SQLAlchemyError as e:
                    transaction.rollback()
                    print(f"ERROR: User {user} couldn't be dropped: {e}.")
    
    else:
        print("Access denied. Incorrect password.")


def drop_roles(config, connection):
    """
    Gets database configuration parameters from the given
    "config.yml" file and drops existing roles from the list
    of schema names as specified in the schema key.
    """
   
    # read the configuration
    configuration = read_configuration(config)

    # PostgreSQL connection information
    conn_string = db_connect.get_cluster_connection(connection)

    # Create the SQLAlchemy engine
    engine = create_engine(conn_string)

    # Authorize with a confirmation prompt
    user_confirm = input("Enter Y to proceed: ")

    # Compare the user's input with the validation rule
    if user_confirm == 'Y':
        print("Access granted. Roles will be dropped.")

    # Itereate through the list of schema names and delete the respective roles
    for schema in configuration['schemas']:

        # Collect drop statements for the given schema
        schema_roles = create_schema_roles(schema, connection)
        statements = schema_roles['drop_statements_all'] + \
                     schema_roles['drop_statements_use'] + \
                     schema_roles['drop_statements_r']
        
        # Drop the schema roles on the cluster
        for statement in statements:
            with engine.connect() as conn:
                transaction = conn.begin()
                try:
                    conn.execute(statement[1])
                    transaction.commit()
                    print("INFO: Role dropped.")
                except SQLAlchemyError as e:
                    print(f"ERROR: {e}")
                continue