import yaml
import os


def read_configuration(config):
    """
    Gets database configuration parameters from the given
    "config.yml" file and creates a configuration dictionary
    to be used in other functions.
    """

    # read the configuration
    with open(config) as file:
        configuration = yaml.safe_load(file)

    # create paths for the local folder structure
    paths = {}
    paths['doc_path'] = configuration['doc_path']
    paths['log_path'] = configuration['log_path']
    paths['secrets_path'] = configuration['secrets_path']

    # append paths to configuration dictionary
    configuration['paths'] = paths

    # create filenames for the create, undo and log files
    files = {}
    files['setup_statements'] = os.path.join(paths['doc_path'], 'setup_statements.sql')
    files['undo_statements'] = os.path.join(paths['doc_path'], 'undo_statements.sql')
    files['secrets'] = os.path.join(paths['secrets_path'], 'secrets.txt')
    files['log'] = os.path.join(paths['log_path'], 'log.log')

    # append files to configuration dictionary
    configuration['files'] = files

    return configuration
