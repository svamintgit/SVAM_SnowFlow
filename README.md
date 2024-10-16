# Bobsled

## Overview

**Bobsled** is a command-line tool that simplifies and automates deployments, task management, and script execution within Snowflake environments. It allows users to easily manage Snowflake resources by streamlining object creation, data management, and running tasks like DAGs. Bobsled is built for scalable data pipeline management across multiple environments, integrating seamlessly with Snowflake APIs.

## Installation

To install Bobsled, use the following command:

```bash
pip install bobsled
```

Ensure that you have configured your `connections.toml` and `query_variables.yaml` properly before running the app.

## Features

Bobsled supports a variety of commands to make Snowflake object management and deployment smoother. Below are the key commands, explanations, and examples of how to use them.

Given your `commands.py` file, here’s a consistent explanation for each command that you can use for the README documentation.

### 1. `init`

The `init` command is responsible for initializing the Snowflake environment, databases, or schemas. It sets up folder structures and prepares the account for deployment.

- **Usage**: 
```bash
bobsled init -e <environment> -d <database> -s <schema>
```
- **Options**:
  - `-e`: Environment 
  - `-d`: Database name
  - `-s`: Schema name

### 2. `deploy`

The `deploy` command allows you to deploy Snowflake objects like databases and schemas. The level of deployment depends on the arguments passed.

- **Usage**: 
```bash
bobsled deploy -e <environment> -d <database> -s <schema>
```
- **Options**:
  - `-e`: Environment
  - `-d`: Database name (deploys the database if no schema is specified)
  - `-s`: Schema name (deploys a specific schema within the database)

### 3. `clone`

The `clone` command allows cloning of Snowflake databases or schemas.

- **Usage**:
```bash
bobsled clone -e <environment> -sd <source_db> -ss <source_schema> -td <target_db> -ts <target_schema>
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
bobsled run_script -e <environment> -d <database> -s <schema> -f <file_path>
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
bobsled test_dag -e <environment> -d <database> -s <schema> -f <dag_file>
```
- **Options**:
  - `-e`: Environment
  - `-d`: Database name
  - `-s`: Schema name
  - `-f`: DAG file path

Each function has error handling for scenarios such as invalid environments or database errors to ensure smooth execution.

Here’s a markdown section that explains the authentication methods Bobsled supports, including the steps for RSA key-pair authentication:

## Authentication Methods

Bobsled supports two authentication methods for connecting to Snowflake:

1. **Username and Password**
2. **RSA Key-Pair Authentication**

### 1. Username and Password Authentication

This is the default and most straightforward authentication method. All the necessary information is provided in the `connections.toml` file.

### 2. RSA Key-Pair Authentication

RSA Key-Pair Authentication is a more secure alternative to username/password. It requires generating a private-public key pair, configuring Snowflake to use your public key, and using your private key for authentication in Bobsled.

#### Steps to Set Up RSA Key-Pair Authentication:

1. **Generate RSA Key-Pair:**
   You can generate a new RSA key-pair using OpenSSL:
   ```bash
   openssl genrsa -out rsa_key.pem 2048
   ```

2. **Convert Private Key to DER Format:**
   Snowflake requires the private key in DER format. Use the following command to convert your `.pem` key:
   ```bash
   openssl pkcs8 -topk8 -inform PEM -outform DER -in rsa_key.pem -out rsa_key.der -nocrypt
   ```

3. **Upload the Public Key to Snowflake:**
   Extract the public key from the private key and upload it to Snowflake:
   ```bash
   openssl rsa -in rsa_key.pem -pubout -out rsa_key.pub
   ```
   
   In Snowflake, run the following command to associate the public key with your Snowflake user:
   ```sql
   ALTER USER your_username SET rsa_public_key = 'your_public_key_contents';
   ```

5. **Test the Connection:**
   Once you’ve updated your `connections.toml` and added the public key to Snowflake, test the connection by running a Bobsled command:
   ```bash
   bobsled deploy -e <environment> -d <database> -s <schema>
   ```

## Configuration Files

Bobsled requires a few configuration files to define how it interacts with Snowflake, including environments and variable settings.

### 1. `connections.toml`

The `connections.toml` file defines the connection settings for each environment, including Snowflake credentials and environment-specific details. The example configuration below shows the configuration setup for both user/password and RSA key-pair.

#### Example Configuration:
```toml
[environment_name]
name = "user"
account = "your_snowflake_account_url"
user = "USERNAME"
authenticator = "snowflake_jwt"
private_key_path = "local path to private key"
database = "your_database"
warehouse = "your_warehouse"
role = "your_role"

[evironment_name] 
name = "user"
account = "your_snowflake_account_url"
user = "USERNAME"
authenticator = "snowflake"
password = "your_password"
database = "your_database"
warehouse = "your_warehouse"
role = "your_role"
```

### 2. `query_variables.yaml`

This file contains environment-specific configuration variables that Bobsled uses during deployments and to validate environment names during command execution. Bobsled requires a `query_variables.yaml` file to be present in the directory where you are running the app.

#### Example:
```yaml
dev:
  '!!!storage_url!!!': 
  '!!!ENABLED!!!': 'TRUE'

prd:
  '!!!storage_url!!!': 
  '!!!ENABLED!!!': 'TRUE'
```

Here’s a revised section on the **Pipeline YAML File** for the README:

```markdown
### 3. Pipeline YAML File

Bobsled can be integrated into CI/CD pipelines to automate the deployment process, manage different environments (such as development and production), and ensure continuous deployment to Snowflake. Below is an example of a YAML file used in an Azure DevOps pipeline. This file can be modified according to your project’s specific needs, such as using environment variables, custom branches, and deployment commands.

#### Example Azure DevOps Pipeline Configuration:
```yaml
trigger:
  branches:
    include:
      - dev
      - prd  

pool:
  vmImage: 'ubuntu-latest'  

# Install the correct Python version
steps:
- task: UsePythonVersion@0
  displayName: 'Set Python version to 3.9'
  inputs:
    versionSpec: '3.9.x'

# Download the required secure files 
- task: DownloadSecureFile@1
  inputs:
    secureFile: 'connections.toml'
  displayName: 'Download connections.toml'

# Move connections.toml file and rsa_key.der to correct location
# Dynamically update the private key path in connections.toml
# Extract local private_key_path and replace it with Linux path
- script: |
    mkdir -p ~/.snowflake
    mv $(Agent.TempDirectory)/connections.toml ~/.snowflake/connections.toml
    mv $(Agent.TempDirectory)/rsa_key.der ~/.snowflake/rsa_key.der
    local_key_path = $(grep 'private_key_path' ~/.snowflake/connections.toml | awk -F'=' '{print $2}' | tr -d ' "')
    sed -i 's|C:/Users/XYZ/rsa_key.der|/home/vsts/.snowflake/rsa_key.der|g' ~/.snowflake/connections.toml
  displayName: 'Move files to Snowflake folder'

# Install Python dependencies and Bobsled
- script: |
    pip install -r requirements.txt
  displayName: 'Install dependencies and Bobsled'

# Deploy the changes using Bobsled for the development branch
- task: Bash@3
  displayName: 'Run Bobsled deployment for dev'
  inputs:
    targetType: 'inline'
    script: |
      bobsled deploy -e dev_environment -d your_database -s your_schema
  condition: eq(variables['Build.SourceBranchName'], 'dev')

# Deploy the changes using Bobsled for the production branch
- task: Bash@3
  displayName: 'Run Bobsled deployment for production'
  inputs:
    targetType: 'inline'
    script: |
      bobsled deploy -e prd_environment -d your_database -s your_schema
  condition: eq(variables['Build.SourceBranchName'], 'prd')
```

### File Structure for SQL Scripts

When deploying with Bobsled, your SQL scripts must be located in the following directory structure relative to your current working directory:

├── snowflake ├── databases ├── <database_name> ├── schemas ├── <schema_name> ├── init.sql ├── grants.sql ├── file_formats ├── tables ├── stages ├── streams ├── views ├── tasks ├── udfs ├── stored_procs ├── post_deploy


For example, if you are deploying a schema called `test_schema` under a database called `demo`, Bobsled will look for the SQL scripts under:

snowflake/databases/demo/schemas/test_schema/

Make sure to organize your SQL scripts according to this structure to ensure correct deployment.

## License

Bobsled is licensed under the [BSD 3-Clause License]