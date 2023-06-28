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
    paths['db_path'] = os.path.join(configuration['base_path'], configuration['db_name'])
    paths['db_details_path'] = os.path.join(paths['db_path'], '01_database')
    paths['extensions_path'] = os.path.join(paths['db_details_path'], '01_extensions')
    paths['schemas_path'] = os.path.join(paths['db_details_path'], '02_schemas')
    paths['functions_path'] = os.path.join(paths['db_details_path'], '03_functions_and_procedures')
    paths['roles_path'] = os.path.join(paths['db_path'], '02_roles')
    paths['access_roles_path'] = os.path.join(paths['roles_path'], 'access_roles')
    paths['users_path'] = os.path.join(paths['roles_path'], 'users')
    paths['doc_path'] = configuration['doc_path']
    paths['log_path'] = configuration['log_path']

    # append paths to configuration dictionary
    configuration['paths'] = paths

    # create path to secrets.txt
    configuration['secret_path'] = os.path.join(configuration['paths']['db_path'], 'secrets.txt') 

    return configuration