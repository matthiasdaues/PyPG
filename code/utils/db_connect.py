import yaml

def get_cluster_connection(connection: str):
    """
    Gets postgres cluster connection parameters and returns a connection string.
    """
    cluster = yaml.safe_load(open(connection))
    user = cluster['db-user']
    pwd = cluster['db-pass']
    host = cluster['db-host']
    database = cluster['db-base']
    port = str(cluster['db-port'])
    conn_string = "postgresql://%s:%s@%s:%s/%s" % (user, pwd, host, port, database)

    return conn_string


def get_db_connection(config: str, connection: str):
    """
    Gets postgres database connection parameters and returns a connection string.
    """
    cluster = yaml.safe_load(open(connection))
    db      = yaml.safe_load(open(config))
    user = cluster['db-user']
    pwd = cluster['db-pass']
    host = cluster['db-host']
    database = db['db_name']
    port = cluster['db-port']
    conn_string = "postgresql://%s:%s@%s:%s/%s" % (user, pwd, host, port, database)

    return conn_string