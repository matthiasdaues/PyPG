import os                                           # noqa: F401

from sqlalchemy import create_engine                # noqa: F401
from sqlalchemy import text, quoted_name            # noqa: F401
from sqlalchemy.exc import SQLAlchemyError          # noqa: F401

import utils.db_connect as db_connect
from core.read_configuration import read_configuration


def create_policies(config: str, connection: str):
    """
    Get database configuration parameters from the given
    "config.yml" file and deploy the database specific
    configurations for schemas.
    """
   
    # read the configuration
    configuration = read_configuration(config)

    # PostgreSQL connection information
    conn_string = db_connect.get_db_connection(config, connection)

    # Create the SQLAlchemy engine
    engine = create_engine(conn_string)

    # deploy policies per user
    policies = configuration['policies']
    for user, policy in policies.items():
        for schema, access_tier in policy.items():
            
            # construct the necessary grant statements
            alter_def_priv_all = text(f"alter default privileges for role {quoted_name(user, False)} in schema {quoted_name(schema, False)} grant all on tables to {quoted_name(schema, False)}_all")
            alter_def_priv_use = text(f"alter default privileges for role {quoted_name(user, False)} in schema {quoted_name(schema, False)} grant select, insert, update, delete on tables to {quoted_name(schema, False)}_use")
            alter_def_priv_r = text(f"alter default privileges for role {quoted_name(user, False)} in schema {quoted_name(schema, False)} grant select on tables to {quoted_name(schema, False)}_r")
            grant_privilege = text(f"grant {quoted_name(schema, False)}_{quoted_name(access_tier, False)} to {quoted_name(user, False)};")

            # enable every user designated to have the "all" access tier to 
            # grant the tier specific access privileges on future objects
            if access_tier == 'all':
                with engine.connect() as conn:
                    transaction = conn.begin()
                    try:
                        conn.execute(alter_def_priv_all)
                        conn.execute(alter_def_priv_use)
                        conn.execute(alter_def_priv_r)
                        conn.execute(grant_privilege)
                        transaction.commit()                        
                        print(f"INFO: User {user} has been granted privilege {schema}_{access_tier}")
                        print(f"INFO: Default privileges for future objects in schema altered.")
                    except SQLAlchemyError as e:
                        print(f"ERROR: {e}")
                    continue
            # grant all other policies without altering default privileges
            else:
                with engine.connect() as conn:
                    transaction = conn.begin()
                    try:
                        conn.execute(grant_privilege)
                        transaction.commit()
                        print(f"INFO: User {user} has been granted privilege {schema}_{access_tier}")
                    except SQLAlchemyError as e:
                        print(f"ERROR: {e}")
                    continue


            # create sql statements in the respective policy folders