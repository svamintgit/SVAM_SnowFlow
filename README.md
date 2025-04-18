# SnowFlow

## Overview

**Snowflow** is a command-line tool that simplifies and automates deployments, task management, and script execution within Snowflake environments. It allows users to easily manage Snowflake resources by streamlining object creation, data management, and running tasks like DAGs. Snowflow is built for scalable data pipeline management across multiple environments, integrating seamlessly with Snowflake APIs.

## Installation

To install Snowflow, use the following command:

```bash
pip install snowflow
```

### Usage

For general help:
```bash
snowflow -h
```

For command-specific help:
```bash
snowflow <command> -h
```

Ensure that you have configured your `connections.toml` properly before running the app.

## Features

Snowflow supports a variety of commands to make Snowflake object management and deployment smoother. Below are the key commands, explanations, and examples of how to use them.

### 1. `init`

The `init` command is responsible for initializing the Snowflake environment, databases, or schemas. It sets up folder structures and prepares the account for deployment. The environment flag `-e` is not required for `init`.

- **Usage**: 
```bash
snowflow init -d <database> -s <schema>
```
- **Options**:
  - `-d`: Database name
  - `-s`: Schema name

### 2. `deploy`

The `deploy` command allows you to deploy Snowflake objects like databases and schemas. The level of deployment depends on the arguments passed.

- **Usage**: 
```bash
snowflow deploy -e <environment> -d <database> -s <schema>
```
- **Options**:
  - `-e`: Environment
  - `-d`: Database name (deploys the database if no schema is specified)
  - `-s`: Schema name (deploys a specific schema within the database)

### 3. `clone`

The `clone` command allows cloning of Snowflake databases or schemas.

- **Usage**:
```bash
snowflow clone -e <environment> -sd <source_db> -ss <source_schema> -td <target_db> -ts <target_schema>
```
- **Options**:
  - `-sd`: Source database
  - `-ss`: Source schema (optional, if not provided the entire database is cloned)
  - `-td`: Target database
  - `-ts`: Target schema (optional)

### 4. `run_script`

The `run_script` command executes a specific SQL script in the provided Snowflake environment.

- **Usage**:
```bash
snowflow run_script -e <environment> -d <database> -s <schema> -f <file_path>
```
- **Options**:
  - `-e`: Environment 
  - `-d`: Database name
  - `-s`: Schema name
  - `-f`: File path for the script

### 5. `test_dag`

This command allows testing of a specific DAG (Directed Acyclic Graph) to verify that all dependent tasks execute as expected.

- **Usage**:
```bash
snowflow test_dag -e <environment> -d <database> -s <schema> -f <dag_file>
```
- **Options**:
  - `-e`: Environment
  - `-d`: Database name
  - `-s`: Schema name
  - `-f`: DAG file path

Each function has error handling for scenarios such as invalid environments or database errors to ensure smooth execution.

## Environment Management

Snowflow supports managing environments in two ways: separate accounts or separate databases. This flexibility allows organizations to align Snowflow's configuration with their Snowflake architecture, depending on their deployment needs and security considerations.

1. **Separate Accounts**

In this setup, each environment (such as dev, test, or prd) corresponds to a distinct Snowflake account. Each account has its own isolated set of credentials, data, and configurations.

* **Use Case**: Recommended for organizations with strong environment isolation requirements.
* **Environment Parameter**: The -e parameter specifies the account-specific environment (e.g., -e dev or -e prd), which maps to configurations in connections.toml for each account.
* **query_variables.yaml**: Can define account-specific variables, such as account URLs and specific resource configurations.

2. **Separate Databases within a Single Account**

In this approach, all environments exist within a single Snowflake account but are isolated by database names (example: dev_db, test_db, and prd_db).

* **Use Case**: Suitable for teams that want environment separation within a single account for ease of access and reduced account management overhead.
* **Environment Parameter**: The -e parameter specifies the target database environment (e.g., -e dev or -e prd), allowing Snowflow to point to the correct resources for each database within the account.
* **query_variables.yaml**: This file can specify different database configurations and variables for each environment.

Both methods require that the specified environment in connections.toml matches the intended account or database setup. This mapping, along with the `-e` parameter, ensures Snowflow performs operations in the correct environment.

## Authentication Methods

Snowflow supports two authentication methods for connecting to Snowflake:

1. **RSA Key-Pair Authentication**
2. **SSO (Single Sign-On) with Token Caching**


### 1. RSA Key-Pair Authentication

RSA Key-Pair Authentication is a more secure alternative to username/password. It requires generating a private-public key pair, configuring Snowflake to use your public key, and using your private key for authentication in Snowflow.
https://docs.snowflake.com/en/user-guide/key-pair-auth#generate-the-private-key

#### Steps to Set Up RSA Key-Pair Authentication:

1. **Generate RSA Key-Pair:**
   Follow the instructions from Snowflake to generate an encrypted private key pair, and make sure to note the password used. Apply the public key to the desired user

2. **Test the Connection:**
   Once you have updated your `connections.toml` and added the public key to Snowflake, test the connection by running a Snowflow command:
   ```bash
   snowflow deploy -e <environment> -d <database> -s <schema>
   ```

### 2. SSO (Single Sign-On) with Token Caching

This method uses a given Single Sign-On provider of your choice and securely caches a session token to reduce the need for repeated authentications. The first time you connect using SSO, Snowflow will authenticate via an external browser and store the session token in a secure cache file for future use. If the cached token is valid, Snowflow will use it for future connections. If not, it will re-authenticate through the browser, update the cache with a new token, and resume the connection. 


## Configuration Files

Snowflow requires `connections.toml` to define how it interacts with Snowflake. `query_variables.yaml` should be present in the repository, but usage is optional. Below are setup instructions and explanations for both.

### 1. `connections.toml`

The `connections.toml` file defines the connection settings for each environment, including Snowflake credentials and environment-specific details. The example configuration below shows the configuration setup for user/password, RSA key-pair and SSO - in that order.

#### Example Configuration:
```toml
[environment_using_rsa]
account = "<snowflake org>-<snowflake account>"
user = "<username>"
authenticator = "snowflake_jwt"
private_key_path = "<local path to private key file>.p8"
private_key_file_pwd = "<password for private key file>"
database = "<your_database>"
warehouse = "<your_warehouse>"
role = "<your_role>"


[environment_using_sso] 
account = "<snowflake org>-<snowflake account>"
user = "<username>"
authenticator = "externalbrower"
database = "<your_database>"
warehouse = "<your_warehouse>"
role = "<your_role>"
```

### 2. `query_variables.yaml`

The `query_variables.yaml` file allows users to define environment-specific variables that Snowflow can substitute into SQL queries or YAML files at runtime. This approach enables flexible, environment-dependent configurations without hardcoding values directly into code or query files.

#### Use Cases

1. **Environment-Specific Configuration:**

  * Define environment-dependent values such as URLs, enabled states, and file paths.
  * Facilitate deployments across multiple environments (example: dev, prd) with different configurations.

2. **Query Substitutions:**

  * Insert specific values (e.g., storage_url or ENABLED flags) into SQL queries dynamically, improving adaptability and avoiding repetitive updates across environments.

3. **Flexible Settings Management:**

  * Easily manage and update configurations for each environment from a single file, enabling smooth deployment and testing transitions.

#### Preferred Naming Style

Variable names within query_variables.yaml can be in any format, but the !!!variable_name!!! style is preferred as it stands out within SQL code. However, this naming style is optional.

#### Example Content for `query_variables.yaml`:
```yaml
dev_env (should match a name in the connections.toml file):
  '!!!storage_url!!!': YOUR_STORAGE_URL
  '!!!ENABLED!!!': 'FALSE'
  '!!!WAREHOUSE!!!': DEV_WH

tst_env (should match a name in the connections.toml file):
  '!!!storage_url!!!': YOUR_STORAGE_URL
  '!!!ENABLED!!!': 'TRUE'
  '!!!WAREHOUSE!!!': TST_WH
```
In this example:
* The !!!storage_url!!! and !!!ENABLED!!! variables are assigned values specific to each environment. Snowflow will replace these values in the scripts depending on the environment. 

#### Usage Examples

1. **SQL Example**

In a SQL file, you might use `query_variables.yaml` values as placeholders to be substituted:

```sql
CREATE OR REPLACE STAGE my_stage
  URL = '!!!storage_url!!!'
  ENABLED = '!!!ENABLED!!!';
```
In this case:

* !!!storage_url!!! will be replaced with the value in query_variables.yaml for the current environment.
* !!!ENABLED!!! will also be replaced with the corresponding environment-specific value.

2. **YAML Example**

You can set up a dag YAML file like below:

```yaml
DAG_NAME: <name>
SCHEDULE: USING CRON 0 8 * * * America/New_York
ROOT_TASK: root
WAREHOUSE: '!!!WAREHOUSE!!!'
ALLOW_OVERLAPPING_EXECUTION: 'FALSE'
ENABLED: '!!!ENABLED!!!'
```
In this case:

* The values for url and enabled would be replaced dynamically based on the environment, pulling from `query_variables.yaml`.


### 3. Pipeline YAML File

Snowflow can be integrated into CI/CD pipelines to automate the deployment process, manage different environments (such as development and production), and ensure continuous deployment to Snowflake. Below is an example of a YAML file used in an Azure DevOps pipeline. This file can be modified according to the specific needs of your project, such as using environment variables, custom branches, and deployment commands.

#### Example Azure DevOps Pipeline Configuration:
```
trigger:
  branches:
    include:
    - <dev_branch>
    - <tst_branch>
    - <prd_branch>
pool:
  vmImage: 'ubuntu-latest'
variables:
  ${{ if eq(variables['Build.SourceBranchName'], 'oca_datamart_dev') }}:
    snow_env: DEV_ENV
  ${{ if eq(variables['Build.SourceBranchName'], 'oca_datamart_tst') }}:
    snow_env: TST_ENV
  ${{ if eq(variables['Build.SourceBranchName'], 'oca_datamart_prd') }}:
    snow_env: PRD_ENV
steps:
- task: UsePythonVersion@0
  displayName: 'Use Python 3.9.x'
  inputs:
    versionSpec: '3.9.x'
- task: DownloadSecureFile@1
  name: connectionsfile
  displayName: ' Download connections.toml'
  inputs:
    secureFile: 'connections.toml' 
    
- task: DownloadSecureFile@1
  name: rsakey
  displayName: 'Download RSA private key'
  inputs:
    secureFile: 'devops_rsa_key.p8'
- script: |
    mkdir -p ~/.snowflake
    echo $(rsakey.secureFilePath)
    echo $(connectionsfile.secureFilePath)
    chmod 644 $(rsakey.secureFilePath)
    mv $(connectionsfile.secureFilePath) ~/.snowflake/connections.toml
    chmod 644 ~/.snowflake/connections.toml
  displayName: 'Move files to Snowflake folder'

# Install Python dependencies and Snowflow
- script: |
    pip install snowflow --upgrade
  displayName: 'Install dependencies and Snowflow'

- task: Bash@3
  displayName: 'Run Snowflow deployment for dev'
  inputs:
    targetType: 'inline'
    script: |
      snowflow deploy -e $(snow_env) -d <database name> -s <schema name>
```

### File Structure for SQL Scripts

When deploying with Snowflow, your SQL scripts must be located in the following directory structure relative to your current working directory. For example, if you are deploying a schema called `test_schema` under a database called `demo`, Snowflow will look for the SQL scripts under:
```
snowflake/databases/demo/schemas/test_schema/
```
Make sure to organize your SQL scripts according to this structure to ensure correct deployment.

## Authors

* Thomas Garcia - tgarcia@svam.com
* Aryan Singh - aryan.singh@svam.com

## License

Snowflow is licensed under the [BSD 3-Clause License]