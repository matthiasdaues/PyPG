import core

# declare the paths to the configuration yaml and
# the connection yaml files.
config = '../inputs/config.yml'
connection = '../inputs/connection.yml'

# deploy database as specified in the configuration
# and connection yaml files.
# configuration = core.read_configuration(config)
# print(configuration)

# create the local folders for documentation and 
# logging. Create the temporary password store.
core.create_local_folders_and_files(config)

# create database on cluster as specified in the connection.yml
core.create_database(config, connection)

# create extensions in the database.
core.create_extensions(config, connection)

# create the users (login roles) if they don't already
# exist on the pg-cluster.
core.create_users(config, connection)

# create the schemas in the database along with their
# respective access tiers.
core.create_schemas(config, connection) 

# grant the respective privileges per user on the schemas
# as defined in the three access tiers.
core.create_policies(config, connection)