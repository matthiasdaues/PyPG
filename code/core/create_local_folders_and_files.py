import os

from core.read_configuration import read_configuration


def create_local_folders_and_files(config):
    """
    Gets database configuration parameters from the given
    "config.yml" file and creates the necessary folders
    and files to store the configuration details.
    """

    # read the configuration
    configuration = read_configuration(config)

    # create path dictionary
    paths = configuration['paths']

    # create the base directory for the configuration's files
    for key in paths:
        path = paths[key]
        try:
            if not os.path.exists(path):
                os.makedirs(path)
                print(f"INFO: Path '{path}' has been created.")
            else:
                print(f"INFO: Path '{path}' already exists.")
        except OSError as error:
            print(f"ERROR: Error creating path '{path}': {error}")
        continue

    # create the secrets.txt file
    secret_path = configuration['secret_path']
    try:
        if not os.path.exists(secret_path):
            open(secret_path, "x")
            print("INFO: secret.txt has been created.")
        else:
            print("INFO: secret.txt already exists.")
    except OSError as e:
        print(f"ERROR: Error creating secret.txt at {secret_path}: {e}")
