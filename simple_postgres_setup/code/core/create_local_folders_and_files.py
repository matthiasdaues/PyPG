import os

from .read_configuration import read_configuration
from simple_postgres_setup.code.utils import write_to_log


def create_local_folders_and_files(config):
    """
    Gets database configuration parameters from the given
    "config.yml" file and creates the necessary folders
    and files to store the configuration details.
    """

    # read the configuration
    configuration = read_configuration(config)

    # extract the paths and files dictionary
    paths = configuration['paths']
    files = configuration['files']
    db_name = configuration['db_name']

    # create the directories to put the files in
    for key in paths:
        path = paths[key]
        try:
            if not os.path.exists(path):
                os.makedirs(path)
        except OSError as error:
            print(f"ERROR: Error creating path '{path}': {error}")
        continue

    # create the log file
    log = files['log']
    log_path = paths['log_path']
    try:
        with open(log, "w"): pass   
        message = f"# Log for setup or undo of database {db_name}\n\n"
        write_to_log(log, message)
    except OSError as e:
        message = f"ERROR: Error creating log.log at {log_path}: {e}"
        print(message)

    # create the secrets.txt file
    secrets = files['secrets']
    secrets_path = paths['secrets_path']
    try:
        if not os.path.exists(secrets):
            open(secrets, "w").close()        
            message = f"INFO: secret.txt has been created."
            write_to_log(log, message)
        else:
            message = f"INFO: secret.txt already exists."
            write_to_log(log, message)
    except OSError as e:
        message = f"ERROR: Error creating secret.txt at {secrets_path}: {e}"
        write_to_log(log, message)

    # create the setup_statements.sql file
    setup_statements = files['setup_statements']
    doc_path = paths['doc_path']
    try:
        with open(setup_statements, "w"): pass
        message = f"INFO: setup_statements.sql has been created."
        write_to_log(log, message)
    except OSError as e:
        message = f"ERROR: Error creating setup_statements.sql at {doc_path}: {e}"
        write_to_log(log, message)

    # create the undo_statements.sql file
    undo_statements = files['undo_statements']
    doc_path = paths['doc_path']
    try:
        with open(undo_statements, "w"): pass
        message = f"INFO: undo_statements.sql has been created."
        write_to_log(log, message)
    except OSError as e:
        message = f"ERROR: Error creating undo_statements.sql at {doc_path}: {e}"
        write_to_log(log, message)
