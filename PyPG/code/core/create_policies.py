import os                                           # noqa: F401

from sqlalchemy import create_engine                # noqa: F401
from sqlalchemy import text, quoted_name            # noqa: F401
from sqlalchemy.exc import SQLAlchemyError          # noqa: F401

import PyPG.code.utils.db_connect as db_connect
from PyPG.code.utils.write_to_log import write_to_log
from PyPG.code.utils.write_to_setup_statements import write_to_setup_statements
from PyPG.code.utils.write_to_undo_statements import write_to_undo_statements
from PyPG.code.core.read_configuration import read_configuration


def create_policies(config: str):
    """
    Get database configuration parameters from the given
    "config.yml" file and deploy the database specific
    configurations for schemas.
    """
   
    # read the configuration
    configuration = read_configuration(config)

    # PostgreSQL connection information
    conn_string = db_connect.get_db_connection(config)

    # define define log and statement files
    setup_statements = configuration['files']['setup_statements']
    undo_statements = configuration['files']['undo_statements']
    log = configuration['files']['log']

    # Create the SQLAlchemy engine
    engine = create_engine(conn_string)

    # deploy policies per user
    policies = configuration['policies']
    for user, policy in policies.items():
        for schema, access_tier in policy.items():
            
            # construct the necessary grant statements
            alter_def_priv_all_tables = text(f"alter default privileges for role {quoted_name(user, False)} in schema {quoted_name(schema, False)} grant all on tables to {quoted_name(schema, False)}_all;")
            alter_def_priv_all_sequences = text(f"alter default privileges for role {quoted_name(user, False)} in schema {quoted_name(schema, False)} grant all on sequences to {quoted_name(schema, False)}_all;")
            alter_def_priv_all_functions = text(f"alter default privileges for role {quoted_name(user, False)} in schema {quoted_name(schema, False)} grant all on functions to {quoted_name(schema, False)}_all;")
            alter_def_priv_use_tables = text(f"alter default privileges for role {quoted_name(user, False)} in schema {quoted_name(schema, False)} grant select, insert, update, delete on tables to {quoted_name(schema, False)}_use;")
            alter_def_priv_use_sequences = text(f"alter default privileges for role {quoted_name(user, False)} in schema {quoted_name(schema, False)} grant all on sequences to {quoted_name(schema, False)}_use;")
            alter_def_priv_r = text(f"alter default privileges for role {quoted_name(user, False)} in schema {quoted_name(schema, False)} grant select on tables to {quoted_name(schema, False)}_r;")
            grant_privilege = text(f"grant {quoted_name(schema, False)}_{quoted_name(access_tier, False)} to {quoted_name(user, False)};")

            # construct the necessary revoke statements
            reset_def_priv_all_tables = text(f"alter default privileges for role {quoted_name(user, False)} in schema {quoted_name(schema, False)} revoke all on tables from {quoted_name(schema, False)}_all;")
            reset_def_priv_all_sequences = text(f"alter default privileges for role {quoted_name(user, False)} in schema {quoted_name(schema, False)} revoke all on sequences from {quoted_name(schema, False)}_all;")
            reset_def_priv_all_functions = text(f"alter default privileges for role {quoted_name(user, False)} in schema {quoted_name(schema, False)} revoke all on functions from {quoted_name(schema, False)}_all;")
            reset_def_priv_use_tables = text(f"alter default privileges for role {quoted_name(user, False)} in schema {quoted_name(schema, False)} revoke select, insert, update, delete on tables from {quoted_name(schema, False)}_use;")
            reset_def_priv_use_sequences = text(f"alter default privileges for role {quoted_name(user, False)} in schema {quoted_name(schema, False)} revoke all on sequences from {quoted_name(schema, False)}_use;")
            reset_def_priv_r = text(f"alter default privileges for role {quoted_name(user, False)} in schema {quoted_name(schema, False)} revoke select on tables from {quoted_name(schema, False)}_r;")
            revoke_privilege = text(f"revoke {quoted_name(schema, False)}_{quoted_name(access_tier, False)} from {quoted_name(user, False)};")

            # enable every user designated to have the "all" access tier to 
            # grant the tier specific access privileges on future objects
            if access_tier == 'all':
                with engine.connect() as conn:
                    transaction = conn.begin()
                    try:
                        conn.execute(alter_def_priv_all_tables)
                        write_to_setup_statements(setup_statements, alter_def_priv_all_tables)
                        write_to_undo_statements(undo_statements, reset_def_priv_all_tables)
                        conn.execute(alter_def_priv_all_sequences)                        
                        write_to_setup_statements(setup_statements, alter_def_priv_all_sequences)
                        write_to_undo_statements(undo_statements, reset_def_priv_all_sequences)
                        conn.execute(alter_def_priv_all_functions)                        
                        write_to_setup_statements(setup_statements, alter_def_priv_all_functions)
                        write_to_undo_statements(undo_statements, reset_def_priv_all_functions)
                        conn.execute(alter_def_priv_use_tables)
                        write_to_setup_statements(setup_statements, alter_def_priv_use_tables)
                        write_to_undo_statements(undo_statements, reset_def_priv_use_tables)
                        conn.execute(alter_def_priv_use_sequences)
                        write_to_setup_statements(setup_statements, alter_def_priv_use_sequences)
                        write_to_undo_statements(undo_statements, reset_def_priv_use_sequences)
                        conn.execute(alter_def_priv_r)
                        write_to_setup_statements(setup_statements, alter_def_priv_r)
                        write_to_undo_statements(undo_statements, reset_def_priv_r)
                        conn.execute(grant_privilege)
                        write_to_setup_statements(setup_statements, grant_privilege)
                        write_to_undo_statements(undo_statements, revoke_privilege)
                        transaction.commit()                        
                        message = text(f"INFO: User {user} has been granted privilege {schema}_{access_tier}")
                        write_to_log(log, message)
                        message = text(f"INFO: Default privileges for future objects in schema altered.")
                        write_to_log(log, message)
                    except SQLAlchemyError as e:
                        message = text(f"ERROR: {e}")
                        write_to_log(log, message)
                    continue
            # grant all other policies without altering default privileges
            else:
                with engine.connect() as conn:
                    transaction = conn.begin()
                    try:
                        conn.execute(grant_privilege)
                        transaction.commit()
                        message = text(f"INFO: User {user} has been granted privilege {schema}_{access_tier}")
                        write_to_log(log, message)
                    except SQLAlchemyError as e:
                        message = text(f"ERROR: {e}")
                        write_to_log(log, message)
                    continue
