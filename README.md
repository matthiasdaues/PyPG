# simple_postgres_setup

This package provides two main functions for **managing a PostgreSQL database**:

1. **`setup_database("path_to_config.yml")`**  
   Sets up a PostgreSQL database using:
   - Credentials from your `.env` file  
   - Additional parameters specified in your `config.yml`  

2. **`drop_database("path_to_config.yml")`**  
   Drops (tears down) the same PostgreSQL database, reverting changes made during the setup.

---

## scope

1. define the database name
2. define the database schemas and schema comments
2.1 per schema there will be three default functional roles: 
    - schema_name_all  = all privileges regarding the schema are granted
    - schema_name_use  = usage on existing schema assets are granted
    - schema_name_read = only select on tables is granted
3. define the login roles (users)
4. define the access policies for the users by connecting the login roles with the functional roles per schema (see example configuration in the template subfolder)

---

## usage

- install with pip (see installation) or other package manager
- create directory 
- create config.yml and .env in the directory (use template files provided in the 'template' subfolder of this pacakge) 
- define output directory in your config.yml as a relative path from the directory you execute the functions from, e.g. THIS directory
- provide all required input in the .env and config.yml files
- run setup

---

## Requirements

- Python 3.10+  
- An existing PostgreSQL server/cluster with Postgreql > 13
- A `.env` file containing the following DB connection parameters (e.g., hostname, port, user credentials):

```sh
# database connection variables

HOST=some_host 
PORT='some_port_number'
DB=postgres # this whole software only works with postgres
USER=postgres_or_other_superuser
PWD=some_password
```

- A `config.yml` specifying how to configure the database and where to store output artifacts. 

**NOTE:** An example configuration file is included in the `templates` subfolder of this package: `templates/config.yml.example`

---

## Installation

1. Make sure you have `pip` installed (or use Poetry / another compatible Python package manager).
2. Install from PyPI (or from source) by running:

   ```bash
   pip install simple_postgres_setup
   ```
