import datetime


def write_to_log(log: str, log_statement: str):
    """
    This function creates log entries.
    """

    timestamp = '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
    try:
        with open(log, "a") as log_f:
            log_f.write(timestamp + ' ' + str(log_statement) + f"\n")
        print(log_statement)
    except OSError as error:
        print(timestamp + ' ' + f"ERROR: Error writing to log: {error}")
