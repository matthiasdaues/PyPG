import os

from sqlalchemy import create_engine                # noqa: F401
from sqlalchemy import text, quoted_name            # noqa: F401
from sqlalchemy.exc import SQLAlchemyError          # noqa: F401

import utils.db_connect as db_connect
from core.read_configuration import read_configuration


def grant_subscription_privilege(config, connection):
    """
    Gets database configuration parameters from the given
    "config.yml" file and grants the privilege to create subscriptions
    to the designated user.
    """

    # read the configuration
    configuration = read_configuration(config)

    # PostgreSQL connection information
    conn_string = db_connect.get_db_connection(config, connection)

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
    for user in configuration['replication']:

        # prepare create statement for user
        grant_subscription = text(f"grant pg_create_subscription to {quoted_name(user, False)};")

        # check if user already exists
        if user not in existing_users:
            print(f"INFO: User {user} does not exist.")
        else:
            # grant subscription privilege to user
            with engine.connect() as conn:
                transaction = conn.begin()
                try:
                    conn.execute(grant_subscription)
                    transaction.commit()
                    print(f"INFO: User {user} has been granted subscription privilege.")
                except SQLAlchemyError as e:
                    transaction.rollback()
                    print(f"ERROR: Privilege couldn't be granted to {user}: {e}.")
