import os

from sqlalchemy import create_engine                # noqa: F401
from sqlalchemy import text, quoted_name            # noqa: F401
from sqlalchemy.exc import SQLAlchemyError          # noqa: F401

import utils.db_connect as db_connect
from core.read_configuration import read_configuration
from utils.random_password_generator import generate_password


def create_users(config, connection):
    """
    Gets database configuration parameters from the given
    "config.yml" file and creates the users specified in
    the users key.
    The user passwords are created and stored in the file
    secrets.txt
    """

    # read the configuration
    configuration = read_configuration(config)

    # define db_name and define directory path as absolute path
    secrets = configuration['secret_path']
    users_path = configuration['paths']['users_path']

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
    conn.close()

    # create users if necessary and store the credentials.
    for user in configuration['users']:

        # prepare create statement for user
        password = generate_password(user)
        create_user = text(f"create user {quoted_name(user, False)} with encrypted password '{quoted_name(password, True)}';")
        secret = str(f"{user}: {password}")

        # check if user already exists
        if user in existing_users:
            print(f"INFO: User {user} already exists in the database.")
        else:
            # check if user is already in the secrets.txt
            # open escrets
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
                print(f"INFO: Credentials for {user} have been updated.")

                # Create the user in the database
                with engine.connect() as conn:
                    transaction = conn.begin()
                    try:
                        conn.execute(create_user)
                        transaction.commit()
                        print(f"INFO: User {user} has been created.")
                    except SQLAlchemyError as e:
                        transaction.rollback()
                        print(f"ERROR: User {user} couldn't be created: {e}.")
                conn.close()

                # Create local assets
                create_user_folder(user, users_path)

            else:

                with open(secrets, "a") as write_secrets:
                    write_secrets.write(secret + "\n")
                print(f"INFO: Credentials for user {user} created.")

                # Create the user in the database
                with engine.connect() as conn:
                    transaction = conn.begin()
                    try:
                        conn.execute(create_user)
                        transaction.commit()
                        print(f"INFO: User {user} has been created.")
                    except SQLAlchemyError as e:
                        transaction.rollback()
                        print(f"ERROR: User {user} couldn't be created: {e}.")
                conn.close()

                # Create local assets
                create_user_folder(user, users_path)

######################
# ancillary functions.
######################


def create_user_folder(user: str, users_path: str):
    """
    ancillary function to create local folders.
    """
    # create folder for user related sql
    user_path = os.path.join(users_path, user)
    try:
        if not os.path.exists(user_path):
            os.makedirs(user_path)
            print(f"INFO: Directory {user} created.")
        else:
            print(f"INFO: Directory {user} already exists.")

        # write create and drop statements to file
        create = f"create user {user} with encrypted password 'ThisIsNotThePassword';"
        drop = f"drop user {user};"
        create_file = os.path.join(user_path, '01_create_user.sql')
        drop_file = os.path.join(user_path, '04_drop_user.sql')
        if not os.path.exists(create_file):
            with open(create_file, "w") as create_f:
                create_f.write(create)
            print("INFO: Create file created.")
        else:
            print("INFO: Create file already exists.")
        if not os.path.exists(drop_file):
            with open(drop_file, "w") as drop_f:
                drop_f.write(drop)
            print("INFO: Drop file created.")
        else:
            print("INFO: Drop file already exists.")
    except OSError as e:
        print(f"ERROR: Error creating directory {user}: {e}")
