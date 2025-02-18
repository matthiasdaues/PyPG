import core

# declare the paths to the configuration yaml and
# the connection yaml files.
config = './inputs/config.yml'
connection = './inputs/connection.yml'

# drop database, users and roles
core.drop_database(config)
core.drop_users(config)
core.drop_roles(config)