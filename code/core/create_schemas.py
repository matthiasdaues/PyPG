import os                                           # noqa: F401

from sqlalchemy import create_engine                # noqa: F401
from sqlalchemy import text, quoted_name            # noqa: F401
from sqlalchemy.exc import SQLAlchemyError          # noqa: F401

import utils.db_connect as db_connect
from core.read_configuration import read_configuration


def create_schemas(config: str, connection: str):
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

    # Get list of existing schemas and compare with configuration.
    get_schemas = text("select schema_name from information_schema.schemata;")
    with engine.connect() as conn:
        result = conn.execute(get_schemas)
        existing_schemas = [row[0] for row in result]

    # Get list of existing roles and compare with configuration.
    get_schemas = text("select rolname from pg_catalog.pg_roles;")
    with engine.connect() as conn:
        result = conn.execute(get_schemas)
        existing_roles = [row[0] for row in result]
        print(existing_roles)

    # create local folders for schemas if they don't already exist,
    # create schemas not yet existing in the database,
    # and configure the schema based access roles and privileges
    for schema in configuration['schemas']:

        # define variables for the given schema
        create_schema = text(f"create schema {quoted_name(schema, False)};")
        schema_path = os.path.join(configuration['paths']['schemas_path'], schema) 
        schema_roles = create_schema_roles(schema, connection)

        # create the folders in schema_path
        try:
            if not os.path.exists(schema_path):
                os.makedirs(schema_path)
                print(f"INFO: Path '{schema_path}' has been created.")
            else:
                print(f"INFO: Path '{schema_path}' already exists.")
        except OSError as error:
            print(f"ERROR: Error creating path '{schema_path}': {error}")

        # create schema in db
        if schema in existing_schemas:
            print(f"INFO: Schema {schema} exists.")
        else:
            with engine.connect() as conn:
                transaction = conn.begin()
                try:
                    conn.execute(create_schema)
                    transaction.commit()
                    print(f"INFO: Schema {schema} has been created.")
                except SQLAlchemyError as e:
                    transaction.rollback()
                    print(f"ERROR: Schema {schema} couldn't be created: {e}.")
            
        # create the schema specific roles governing access
        # create access_tier "all" if not exists
        role_all = schema_roles['role_all']
        setup_statements_all = schema_roles['setup_statements_all']
        grant_statements_all = schema_roles['grant_statements_all']
        if role_all in existing_roles:
            print(f"INFO: Role {role_all} already exists on the cluster.")
            for statement in grant_statements_all:
                with engine.connect() as conn:
                    transaction = conn.begin()
                    try:
                        conn.execute(statement[1])
                        transaction.commit()
                        print("INFO: " + str(statement[1]) + " committed")
                    except SQLAlchemyError as e:
                        transaction.rollback()
                        print(f"ERROR: {e}")
                    continue
        else: 
            for statement in setup_statements_all:
                with engine.connect() as conn:
                    transaction = conn.begin()
                    try:
                        conn.execute(statement[1])
                        transaction.commit()
                        print("INFO: " + str(statement[1]) + " committed")
                    except SQLAlchemyError as e:
                        transaction.rollback()
                        print(f"ERROR: {e}")
                    continue
        
        # create access_tier "use" if not exists
        role_use = schema_roles['role_use']
        setup_statements_use = schema_roles['setup_statements_use']
        grant_statements_use = schema_roles['grant_statements_use']
        if role_use in existing_roles:
            print(f"INFO: Role {role_use} already exists on the cluster.")
            for statement in grant_statements_use:
                with engine.connect() as conn:
                    transaction = conn.begin()
                    try:
                        conn.execute(statement[1])
                        transaction.commit()
                        print("INFO: " + str(statement[1]) + " committed")
                    except SQLAlchemyError as e:
                        transaction.rollback()
                        print(f"ERROR: {e}")
                    continue
        else: 
            for statement in setup_statements_use:
                with engine.connect() as conn:
                    transaction = conn.begin()
                    try:
                        conn.execute(statement[1])
                        transaction.commit()
                        print("INFO: " + str(statement[1]) + " committed")
                    except SQLAlchemyError as e:
                        transaction.rollback()
                        print(f"ERROR: {e}")
                    continue
        
        # create access_tier "read" if not exists
        role_r = schema_roles['role_r']
        setup_statements_r = schema_roles['setup_statements_r']
        grant_statements_r = schema_roles['grant_statements_r']
        if role_r in existing_roles:
            print(f"INFO: Role {role_r} already exists on the cluster.")
            for statement in grant_statements_r:
                with engine.connect() as conn:
                    transaction = conn.begin()
                    try:
                        conn.execute(statement[1])
                        transaction.commit()
                        print("INFO: " + str(statement[1]) + " committed")
                    except SQLAlchemyError as e:
                        transaction.rollback()
                        print(f"ERROR: {e}")
                    continue
        else: 
            for statement in setup_statements_r:
                with engine.connect() as conn:
                    transaction = conn.begin()
                    try:
                        conn.execute(statement[1])
                        transaction.commit()
                        print("INFO: " + str(statement[1]) + " committed")
                    except SQLAlchemyError as e:
                        print(f"ERROR: {e}")
                    continue
    
        # create sql statements in the respective schema folders


######################
# ancillary functions.
######################


def create_schema_roles(schema: str, connection: str):
    """
    Create sql statements to model three access tiers for a given schema:
    1. schema_all = master of schema and objects within. Can do all.
    2. schema_use = CRUD usage of data within existing tables using existing functions / procedures.
    3. schema_r   = SELECT on existing tables.
    """
    
    # PostgreSQL connection information
    setup_user = db_connect.get_setup_user(connection)

    ####################
    # Access Tier "all" 
    ####################

    # create lists to collect the statements in their execution order
    create_statements_all = []
    grant_statements_all = []
    revoke_statements_all = []
    drop_statements_all = []

    # create role schema_all
    role_all = schema + '_all'
    drop_statements_all.append(('drop_role_all', text(f"drop role {quoted_name(role_all, False)};")))
    create_statements_all.append(('create_role_all', text(f"create role {quoted_name(role_all, False)} with nosuperuser nocreatedb nologin noreplication;")))
    grant_statements_all.append(('grant_role_all_to_postgres', text(f"grant {quoted_name(role_all, False)} to {quoted_name(setup_user, False)};")))
    revoke_statements_all.insert(0, ('revoke_role_all_from_postgres', text(f"revoke {quoted_name(role_all, False)} from {quoted_name(setup_user, False)};")))

    # grant schema privileges to schema_all
    revoke_statements_all.insert(0, ('revoke_all_from_role_all', text(f"revoke all on {quoted_name(schema, False)} from {quoted_name(role_all, False)};")))
    grant_statements_all.append(('grant_all_to_role_all', text(f"grant all on schema {quoted_name(schema, False)} to {quoted_name(role_all, False)};")))

    # grant object privileges to schema_all
    # This is probably obsolete since at this time there exists no object in the schema.

    # the altering of default privileges will be don in the "policies" function. 
    # It must be run for all three access tiers for every "FOR ROLE" being granted "all" tier access

    ####################
    # Access Tier "use" 
    ####################

    # create lists to collect the statements in their execution order
    create_statements_use = []
    grant_statements_use = []
    revoke_statements_use = []
    drop_statements_use = []
        
    # create role schema_use
    role_use = schema + '_use'
    drop_statements_use.append(('drop_role_use', text(f"drop role {quoted_name(role_use, False)};")))
    create_statements_use.append(('create_role_use', text(f"create role {quoted_name(role_use, False)} with nosuperuser nocreatedb nologin noreplication;")))
    grant_statements_use.append(('grant_role_use_to_postgres', text(f"grant {quoted_name(role_use, False)} to {quoted_name(setup_user, False)};")))
    revoke_statements_use.insert(0, ('revoke_role_use_from_postgres', text(f"revoke {quoted_name(role_use, False)} from {quoted_name(setup_user, False)};")))

    # grant usage on schema to schema_use
    revoke_statements_use.insert(0, ('revoke_usage_on_schema_from_role_use', text(f"revoke usage on schema {quoted_name(schema, False)} from {quoted_name(role_use, False)};")))
    grant_statements_use.append(('grant_usage_on_schema_to_role_use', text(f"grant usage on schema {quoted_name(schema, False)} to {quoted_name(role_use, False)};")))

    ####################
    # Access Tier "read" 
    ####################

    # create lists to collect the statements in their execution order
    create_statements_r = []
    grant_statements_r = []
    revoke_statements_r = []
    drop_statements_r = []
        
    # create role schema_r
    role_r = schema + '_r'
    drop_statements_r.append(('drop_role_r', text(f"drop role {quoted_name(role_r, False)};")))
    create_statements_r.append(('create_role_r', text(f"create role {quoted_name(role_r, False)} with nosuperuser nocreatedb nologin noreplication;")))
    grant_statements_r.append(('grant_role_r_to_postgres', text(f"grant {quoted_name(role_r, False)} to {quoted_name(setup_user, False)};")))
    revoke_statements_r.insert(0, ('revoke_role_r_from_postgres', text(f"revoke {quoted_name(role_r, False)} from {quoted_name(setup_user, False)};")))

    # grant usage on schema to schema_r
    revoke_statements_r.insert(0, ('revoke_usage_on_schema_from_role_r', text(f"revoke usage on schema {quoted_name(schema, False)} from {quoted_name(role_r, False)};")))
    grant_statements_r.append(('grant_usage_on_schema_to_role_r', text(f"grant usage on schema {quoted_name(schema, False)} to {quoted_name(role_r, False)};")))

    # Collate return dictionary with lists of statements to be executed in the main function
    setup_statements_all = create_statements_all + grant_statements_all
    setup_statements_use = create_statements_use + grant_statements_use
    setup_statements_r = create_statements_r + grant_statements_r
    
    statements_dict = {
        'create_statements_all': create_statements_all,
        'create_statements_use': create_statements_use,
        'create_statements_r':  create_statements_r,
        'grant_statements_all': grant_statements_all,
        'grant_statements_use': grant_statements_use,
        'grant_statements_r':  grant_statements_r,
        'revoke_statements_all': revoke_statements_all,
        'revoke_statements_use': revoke_statements_use,
        'revoke_statements_r':  revoke_statements_r,
        'drop_statements_all': drop_statements_all,
        'drop_statements_use': drop_statements_use,
        'drop_statements_r':  drop_statements_r,
        'setup_statements_all': setup_statements_all,
        'setup_statements_use': setup_statements_use,
        'setup_statements_r': setup_statements_r,
        'role_all': role_all,
        'role_use': role_use,
        'role_r': role_r       
    }
        
    # return statements
    return statements_dict

    # for statement in create_statements_all:
    #     print(statement[1])
    # for statement in grant_statements_all:
    #     print(statement[1])
    # for statement in revoke_statements_all:
    #     print(statement[1])
    # for statement in drop_statements_all:
    #     print(statement[1])

    # for statement in create_statements_use:
    #     print(statement[1])
    # for statement in grant_statements_use:
    #     print(statement[1])
    # for statement in revoke_statements_use:
    #     print(statement[1])
    # for statement in drop_statements_use:
    #     print(statement[1])

    # for statement in create_statements_r:
    #     print(statement[1])
    # for statement in grant_statements_r:
    #     print(statement[1])
    # for statement in revoke_statements_r:
    #     print(statement[1])
    # for statement in drop_statements_r:
    #     print(statement[1])