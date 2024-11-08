# Snowflow Demo Project Documentation

## Overview

This demo project illustrates how **Snowflow** can manage data pipelines within Snowflake, demonstrating ingestion, processing, and automation workflows. By integrating data from NYC OpenData, the project showcases Snowflow’s capabilities to simplify Snowflake configurations, manage data transformations, and automate deployment workflows via CI/CD with Azure DevOps.

## Objective of the Demo

The Snowflow demo demonstrates a common workflow:

1. **Data Ingestion**: Fetches NYC Neighborhood Tabulation Areas (NTAs) and NYPD Arrest Data, prepares them for analysis by organizing the data into Snowflake tables.
2. **Data Transformation**: Processes arrest records, linking them with NTAs using geospatial functions.
3. **Automation with DAGs**: Manages dependencies and schedules tasks for continuous processing.
4. **CI/CD Integration**: Uses Azure DevOps to automate deployment in different environments (`dev` and `prd` branches).

By exploring this demo, users can understand how to set up a similar project, configure required Snowflake resources, and automate workflows using Snowflow commands.

## Data Sources

### 1. 2010 Neighborhood Tabulation Areas (NTAs)
- Provides neighborhood data for mapping arrest records.
- [Data Source](https://data.cityofnewyork.us/City-Government/2010-Neighborhood-Tabulation-Areas-NTAs-/cpf4-rkhq)

### 2. NYPD Arrest Data (Year to Date)
- Contains records of arrests, which are categorized by borough.
- [Data Source](https://data.cityofnewyork.us/Public-Safety/NYPD-Arrest-Data-Year-to-Date-/uip8-fykc/about_data)

## Key Snowflow Functionalities Demonstrated

### 1. **Setting Up and Initializing Snowflake Resources**

The demo begins with **Snowflow’s** `init` command, which creates the foundational directories and configurations. This prepares the environment by setting up databases, schemas, roles, and warehouses for structured management of resources.

**Key Commands**:
- `snowflow init -d demo -s opendata` to initialize the primary database and schema structure.
  
### 2. **Loading Data into Snowflake**

Data from the NYPD and NTAs datasets are loaded into Snowflake using staged files in Azure, connected through **external stages**. Snowflow automates this setup using SQL scripts for **data staging** and **file format configurations**.

- **Staging**: External storage in Azure is defined in `snowflow_azure_integration.sql` to bring data from Azure into Snowflake.
- **Data Loading**: COPY INTO statements in SQL files load arrest and NTA data into borough-specific tables (`bronx_arrests`, `brooklyn_arrests`, etc.).

**Key Commands**:
- `snowflow deploy -e mydev -d demo -s opendata` deploys the schema and loads data into Snowflake tables.
  
### 3. **Transforming and Enriching Data with DAGs**

The project uses Snowflow’s DAG configuration to automate the transformation tasks, which link arrest data with NTAs using geospatial operations. This workflow showcases how Snowflow can manage task dependencies and maintain smooth, automated transformations.

- **DAG Workflow**: Defined in `arrests_load_and_process.yaml`, the DAG orchestrates tasks like:
  - Loading borough-specific arrest data.
  - Enriching data by joining arrest records with NTA data to provide neighborhood context.
- **Geospatial Joins**: Each borough's arrests are mapped to NTAs by checking if the arrest’s coordinates fall within NTA boundaries.

**Key Command**:
- `snowflow test_dag -e mydev -d demo -s opendata -f arrests_load_and_process.yaml` validates the DAG tasks in sequence.

### 4. **Creating and Using Views for Analysis**

The demo project also illustrates how to create views to streamline analysis and make data readily accessible for analysts.

- **View Creation**: Views like `bronx_arrests_geo_v` use geospatial joins to display arrests with corresponding NTAs, making neighborhood-level analysis straightforward. A combined view (`all_boroughs_arrests_geo_v`) consolidates data across all boroughs for a unified look.

### 5. **Integrating with CI/CD using Azure DevOps**

The Azure DevOps pipeline in `pipes/demo_opendata_azdo.yaml` automates deployment based on branch changes. It provides seamless integration for updating Snowflake environments, demonstrating how Snowflow fits into a CI/CD workflow.

- **Pipeline Triggers**: The pipeline is triggered on pushes to `dev` or `prd` branches. It:
  - Sets up the environment and installs Snowflow.
  - Deploys the Snowflow project to Snowflake.
- **Environment-Specific Deployments**: `dev` branch changes deploy to `mydev`, while `prd` branch changes deploy to `prdbranch`, ensuring environment isolation.

**Key Pipeline Commands**:
- The pipeline’s `snowflow deploy` command automatically applies schema changes, creates or modifies resources, and executes DAGs.

## Summary

This demo project showcases a typical Snowflow-powered deployment to Snowflake:

- **Initializes structured Snowflake environments** for multi-environment data management.
- **Automates data ingestion, transformation, and enrichment** by defining DAGs with task dependencies.
- **Enables CI/CD-driven deployments** with Azure DevOps, facilitating automated updates to Snowflake environments.