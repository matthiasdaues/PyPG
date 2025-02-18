import os

from sqlalchemy import create_engine                # noqa: F401
from sqlalchemy import text, quoted_name            # noqa: F401
from sqlalchemy.exc import SQLAlchemyError          # noqa: F401

import simple_postgres_setup.code.utils.db_connect as db_connect
from simple_postgres_setup.code.utils.write_to_log import write_to_log
from simple_postgres_setup.code.utils.write_to_setup_statements import write_to_setup_statements
from simple_postgres_setup.code.utils.write_to_undo_statements import write_to_undo_statements
from simple_postgres_setup.code.core.read_configuration import read_configuration


def prepare_subscription(config: str, connection: str):
    """
    Gets database configuration parameters from the given
    "config.yml" file and grants the privilege to create subscriptions
    to the designated user.
    """

    # read the configuration
    configuration = read_configuration(config)

    # define define log and statement files
    setup_statements = configuration['files']['setup_statements']
    undo_statements = configuration['files']['undo_statements']
    log = configuration['files']['log']

    # PostgreSQL connection information
    conn_string = db_connect.get_db_connection(config)

    # Create the SQLAlchemy engine
    engine = create_engine(conn_string)

    # Get list of users to be able to check if a configured username already exists.
    get_users = text("select usename from pg_catalog.pg_user;")
    
    with engine.connect() as conn:
        result = conn.execute(get_users).fetchall()
        existing_users = []
        for i in result:
            existing_users.append(i[0])

    # create users if necessary and store the credentials.
    db_name = configuration['db_name']
    subscription = configuration['subscription']
    for grantee, user_role in subscription.items():

        # prepare create statement for user
        grant_user_role_to_grantee = text(f"grant {quoted_name(user_role, False)} to {quoted_name(grantee, False)}")
        revoke_user_role_from_grantee = text(f"revoke {quoted_name(user_role, False)} from {quoted_name(grantee, False)}")
        grant_create_on_db = text(f"grant create on database {quoted_name(db_name, False)}  to {quoted_name(user_role, False)};")
        revoke_create_on_db = text(f"revoke create on database {quoted_name(db_name, False)}  from {quoted_name(user_role, False)};")
        grant_subscription = text(f"grant pg_create_subscription to {quoted_name(user_role, False)};")
        revoke_subscription = text(f"revoke pg_create_subscription from {quoted_name(user_role, False)};")


        # check if user already exists
        if user_role not in existing_users:
            message = text(f"INFO: User {user_role} does not exist.")
            write_to_log(log, message)
        else:
            
            # grant replication user role to table creator
            with engine.connect() as conn:
                transaction = conn.begin()
                try:
                    conn.execute(grant_user_role_to_grantee)
                    transaction.commit()
                    message = text(f"INFO: User {grantee} has been granted role {user_role}.")
                    write_to_log(log, message)
                    write_to_setup_statements(setup_statements, grant_user_role_to_grantee)
                    write_to_undo_statements(undo_statements, revoke_user_role_from_grantee)
                except SQLAlchemyError as e:
                    transaction.rollback()
                    message = text(f"ERROR: Role {user_role} couldn't be granted to {grantee}: {e}.")
                    write_to_log(log, message)

            # grant create on DB privilege to user
            with engine.connect() as conn:
                transaction = conn.begin()
                try:
                    conn.execute(grant_create_on_db)
                    transaction.commit()
                    message = text(f"INFO: User {user_role} has been granted create privilege on db {db_name}.")
                    write_to_log(log, message)
                    write_to_setup_statements(setup_statements, grant_create_on_db)
                    write_to_undo_statements(undo_statements, revoke_create_on_db)
                except SQLAlchemyError as e:
                    transaction.rollback()
                    message = text(f"ERROR: Privilege couldn't be granted to {user_role}: {e}.")
                    write_to_log(log, message)

            # grant subscription privilege to user
            with engine.connect() as conn:
                transaction = conn.begin()
                try:
                    conn.execute(grant_subscription)
                    transaction.commit()
                    message = text(f"INFO: User {user_role} has been granted subscription privilege.")
                    write_to_log(log, message)
                    write_to_setup_statements(setup_statements, grant_subscription)
                    write_to_undo_statements(undo_statements, revoke_subscription)
                except SQLAlchemyError as e:
                    transaction.rollback()
                    message = text(f"ERROR: Privilege couldn't be granted to {user_role}: {e}.")
                    write_to_log(log, message)
