import os

from sqlalchemy import create_engine                # noqa: F401
from sqlalchemy import text, quoted_name            # noqa: F401
from sqlalchemy.exc import SQLAlchemyError          # noqa: F401

import simple_postgres_setup.code.utils.db_connect as db_connect
from simple_postgres_setup.code.utils.write_to_log import write_to_log
from simple_postgres_setup.code.utils.write_to_setup_statements import write_to_setup_statements
from simple_postgres_setup.code.utils.write_to_undo_statements import write_to_undo_statements
from simple_postgres_setup.code.core.read_configuration import read_configuration
from simple_postgres_setup.code.utils.random_password_generator import generate_password


def create_users(config):
    """
    Gets database configuration parameters from the given
    "config.yml" file and creates the users specified in
    the users key.
    The user passwords are created and stored in the file
    secrets.txt
    """

    # read the configuration
    configuration = read_configuration(config)

    # define secrets, log and statement files
    setup_statements = configuration['files']['setup_statements']
    undo_statements = configuration['files']['undo_statements']
    log = configuration['files']['log']
    secrets = configuration['files']['secrets']

    # PostgreSQL connection information
    conn_string = db_connect.get_db_connection(config)
    setup_user = db_connect.get_setup_user()

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
    for user in configuration['users']:

        # prepare create statement for user
        password = generate_password(user)
        create_user = text(f"create user {quoted_name(user, False)} with encrypted password '{quoted_name(password, False)}';")
        grant_user_to_setup_user = text(f"grant {quoted_name(user, False)} to {quoted_name(setup_user, False)};")
        drop_user_3 = text(f"reassign owned by {quoted_name(user, False)} to {quoted_name(setup_user, False)};")
        drop_user_2 = text(f"drop owned by {quoted_name(user, False)} to {quoted_name(setup_user, False)};")
        drop_user_1 = text(f"drop user {quoted_name(user, False)};")        
        secret = str(f"{user}: {password}")
        write_to_setup_statements(setup_statements, str(create_user) + f"\n" + str(grant_user_to_setup_user))
        write_to_undo_statements(undo_statements, str(drop_user_3) + f"\n" + str(drop_user_2) + f"\n" + str(drop_user_1))

        # check if user already exists
        if user in existing_users:
            message = text(f"INFO: User {user} already exists in the database.")
            write_to_log(log, message)
        else:
            # check if user is already in the secrets.txt
            # open secrets
            with open(secrets, "r") as read_secrets:
                content = read_secrets.read()
            if user in content:
                with open(secrets, "r") as file:
                    lines = file.readlines()

                # Find the line that starts with the search name
                for i, line in enumerate(lines):
                    if line.startswith(user):
                        lines[i] = secret + "\n"
                        break

                # Write the modified lines back to the file
                with open(secrets, "w") as file:
                    file.writelines(lines)
                message = text(f"INFO: Credentials for {user} have been updated.")
                write_to_log(log, message)

                # Create the user in the database
                with engine.connect() as conn:
                    transaction = conn.begin()
                    try:
                        conn.execute(create_user)
                        transaction.commit()
                        message = text(f"INFO: User {user} has been created.")
                        write_to_log(log, message)
                    except SQLAlchemyError as e:
                        transaction.rollback()
                        message = text(f"ERROR: User {user} couldn't be created: {e}.")
                        write_to_log(log, message)

            else:

                with open(secrets, "a") as write_secrets:
                    write_secrets.write(secret + "\n")
                message = text(f"INFO: Credentials for user {user} created.")
                write_to_log(log, message)

                # Create the user in the database
                with engine.connect() as conn:
                    transaction = conn.begin()
                    try:
                        conn.execute(create_user)
                        conn.execute(grant_user_to_setup_user)
                        transaction.commit()
                        message = text(f"INFO: User {user} has been created.")
                        write_to_log(log, message)
                        message = text(f"INFO: Role {user} has been granted to {setup_user}.")
                        write_to_log(log, message)
                    except SQLAlchemyError as e:
                        transaction.rollback()
                        message = text(f"ERROR: User {user} couldn't be created: {e}.")
                        write_to_log(log, message)