import os
import yaml

def get_cluster_connection():
    """
    Gets postgres cluster connection parameters and returns a connection string.
    """
    user = os.getenv('USER')
    pwd  = os.getenv('PWD')
    host = os.getenv('HOST')
    port = os.getenv('PORT')
    db   = os.getenv('DB')
    conn_string = "postgresql://%s:%s@%s:%s/%s" % (user, pwd, host, port, db)   
    return conn_string


def get_db_connection(config: str):
    """
    Gets postgres database connection parameters and returns a connection string.
    """
    config = yaml.safe_load(open(config))
    user = os.getenv('USER')
    pwd  = os.getenv('PWD')
    host = os.getenv('HOST')
    port = os.getenv('PORT')
    db = config['db_name']
    conn_string = "postgresql://%s:%s@%s:%s/%s" % (user, pwd, host, port, db)
    return conn_string


def get_setup_user():
    """
    Gets postgres cluster connection parameters and returns the superuser name.
    """
    setup_user = os.getenv('USER')
    return setup_user