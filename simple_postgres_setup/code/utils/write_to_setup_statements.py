import datetime


def write_to_setup_statements(setup_statements: str, setup_statement: str):
    """
    This function adds sql statements that create
    the database as configured in the config.yml.
    """

    timestamp = '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
    try:
        with open(setup_statements, "a") as setup_f:
            setup_f.write(str(setup_statement) + f"\n")
    except OSError as error:
        print(timestamp + ' ' + f"ERROR: Error writing to setup_statements: {error}")
