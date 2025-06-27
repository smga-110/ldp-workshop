# Databricks notebook source
# DBTITLE 1,Delta Live Table Introduction
# MAGIC %md-sandbox
# MAGIC # Simplify Ingestion and Transformation with Delta Live Tables
# MAGIC
# MAGIC In this workshop, we'll work as a Data Engineer to build our supplier inventory database. <br>
# MAGIC We'll consume and clean our raw data sources to prepare the tables required for our BI and other analytics
# MAGIC
# MAGIC We have 3 data sources sending new files in our blob storage <your Unique User ID Catalog/steyrws_demodata/raw_data and we want to incrementally load this data into our Lakehouse using Delta Live Tables
# MAGIC
# MAGIC - Inventory data (information about parts such as SKU,Category ,etc.)
# MAGIC - Carriers (Shipping Carrier Description)
# MAGIC - Routes (Origin and destination of shipping route)
# MAGIC - Supplier (Supplier Name and other information)
# MAGIC
# MAGIC
# MAGIC Databricks simplifies this task with Delta Live Table (DLT) by making Data Engineering accessible to all.
# MAGIC
# MAGIC DLT allows Data Analysts to create advanced pipelines with plain SQL.
# MAGIC
# MAGIC ## Delta Live Table: A simple way to build and manage data pipelines for fresh, high quality data!
# MAGIC
# MAGIC <div>
# MAGIC   <div style="width: 45%; float: left; margin-bottom: 10px; padding-right: 45px">
# MAGIC     <p>
# MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-accelerate.png"/> 
# MAGIC       <strong>Accelerate ETL development</strong> <br/>
# MAGIC       Enable analysts and data engineers to innovate rapidly with simple pipeline development and maintenance 
# MAGIC     </p>
# MAGIC     <p>
# MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-complexity.png"/> 
# MAGIC       <strong>Remove operational complexity</strong> <br/>
# MAGIC       By automating complex administrative tasks and gaining broader visibility into pipeline operations
# MAGIC     </p>
# MAGIC   </div>
# MAGIC   <div style="width: 48%; float: left">
# MAGIC     <p>
# MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-trust.png"/> 
# MAGIC       <strong>Trust your data</strong> <br/>
# MAGIC       With built-in quality controls and quality monitoring to ensure accurate and useful BI, Data Science, and ML 
# MAGIC     </p>
# MAGIC     <p>
# MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-stream.png"/> 
# MAGIC       <strong>Simplify batch and streaming</strong> <br/>
# MAGIC       With self-optimization and auto-scaling data pipelines for batch or streaming processing 
# MAGIC     </p>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <br style="clear:both">
# MAGIC
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo.png" style="float: right;" width="200px">
# MAGIC
# MAGIC ## Delta Lake
# MAGIC
# MAGIC All the tables we'll create in the Lakehouse will be stored as Delta Lake tables. Delta Lake is an open storage framework for reliability and performance.<br>
# MAGIC It provides many functionalities (ACID Transaction, DELETE/UPDATE/MERGE, Clone zero copy, Change data Capture...)<br>
# MAGIC For more details on Delta Lake, run dbdemos.install('delta-lake')
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&aip=1&t=event&ec=dbdemos&ea=VIEW&dp=%2F_dbdemos%2Flakehouse%2Flakehouse-retail-c360%2F01-Data-ingestion%2F01.1-DLT-churn-SQL&cid=984752964297111&uid=8607129661941341">

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC # Ingestion Pattern: Repeatable for any Scenario
# MAGIC
# MAGIC Databricks simplifies how organizations ingest, transform, and curate data to users through the following pattern:
# MAGIC
# MAGIC 1. Leveraging Lakeflow Connect, Third-Party ETL Tools, or Apache Spark - ingest unaltered data from source into the bronze zone of cloud object storage. This zone will serve as an immutable copy of our source data, which can be used for auditing or pipeline re-runs.
# MAGIC
# MAGIC 2. Automatically and incrementally ingest new data from bronze zone using Auto-Loader, which manages the following:
# MAGIC   - Tracks files as their ingested, ensuring data is only processed once. In the event of failures, resume where it last left off through its fault-tolerant, checkpointing mechanisms.
# MAGIC   - Infer Schema and handle bad records
# MAGIC   - Seamlessly handle Schema Evolution.
# MAGIC
# MAGIC   These are only some of the benefits of Databricks Autoloader. Refer to Databricks Documentation for more details on its Capabilities.
# MAGIC
# MAGIC 3. Using Delta Live Tables, Efficiently capture and process change records with capabilities such as CDC, Materialized Views to automatically update downstream tables as new data arrives.
# MAGIC
# MAGIC This information will then be consumed using highly-performant, cost-effect, AI-Powered DB SQL. In addition, we'll leverage Unity Catalog and Genie to enable consumption of data using Natural Language - democratizing consumption of data to all, regardless of techincal skillset
# MAGIC
# MAGIC The Architecture below showcases how we'll leverage this pattern to performantly ingest and process data:
# MAGIC
# MAGIC  <img src="./images/demo_arch.png"/> 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Run Set-Up Notebook that will create a catalog and generate fake data

# COMMAND ----------

# MAGIC %run ./00-Environment-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. With the fake data generated and schema's prepared, lets proceed to build our DLT Pipeline. Navigate to `01-Data-Ingestion Folder` and select the first notebook.