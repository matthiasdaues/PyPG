import datetime


def write_to_undo_statements(undo_statements: str, undo_statement: str):
    """
    This function creates log entries.
    """

    # the undo statements should be executable in reverse order to 
    # the create statements. To achieve this the content of the
    # existing file is read into memory. 
    with open(undo_statements, "r") as original:
        data = original.read()

    # Then a new file of the same name is opened with the
    # "w" option and the current undo statement is written 
    # as first line and then the content of the old file is 
    # appended to it.
    timestamp = '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
    try:
        with open(undo_statements, "w") as undo_f:
            undo_f.write(str(undo_statement) + f"\n" + data)
    except OSError as error:
        print(timestamp + ' ' + f"ERROR: Error writing to undo_statements: {error}")
