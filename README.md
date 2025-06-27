# ldp-workshop
Interactive workshop for Lakeflow Declarative Pipelines (Formerly DLT). This code base is comprised of 3 core parts:
- Resources: Contains notebooks that will set up catalogs, schemas, and volumes where fake data will reside. The data is mock supply chain data meant created using the Faker Library
    - 00-Environment-Setup: Notebook for setting up catalogs, schemas, and generating inital data.
    - 01-Data-Ingestion: Contains DLT SQL code for the transformation pipeline that will ingest data.
    - 02-Incremental-Data: For generating incremental data to ingest once pipeline is fully completed.
    

# Prerequisites
- Users participating in the workshop will require at the minimum create schema privileges within a precreated catalog. Alternatively, you can alter ./Resources/00-Environment-Setup and ./Resource/02-Incremental-Data to create a catalog on the users behalf.
- Recommended when creating the DLT pipelines to use Serverless Compute

# Instructions
1. Clone this repo within the workspace
2. Navigate to the resources/00-Environment-Setup notebook and click execute
3. Validate that a catalog/schema/volume has been created
4. Navigate to the DLT pipeline creation page, create a Serverless DLT pipeline that uses the code in /01-Data-Ingestion/1.1 Ingestion Source Data using Autoloader
5. Update the notebook throughout the workshop and navigate back to the DLT notebook to run
6. Schedule the notebook with Workflows
7. Once all tables are created. Query using /Databricks SQL/Analysis.sql script

