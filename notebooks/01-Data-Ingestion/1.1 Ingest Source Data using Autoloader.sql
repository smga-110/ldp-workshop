-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ## DLT Core Concepts
-- MAGIC
-- MAGIC Within DLT, there are 3 core ways of representing and work with data
-- MAGIC
-- MAGIC
-- MAGIC 1. <b>Streaming Tables</b> - Delta Table that supports ingesting incremental or streamed data. Data from Sources that are append-only, such as message brokers and files arriving in Cloud Storage are best represented as this. As New Data Arrives, the table will be updated, incrementally. Also ideal when each new data row must only be processed once.
-- MAGIC
-- MAGIC 2. <b>Materialized Views</b> - Where transformations such as aggregation or general CDC are expressed. Similar to other implementations of MV in the sense results are precomputed, however Databricks has the ability to update incrementally instead of doing a full table scan on source (depending source of the MV)
-- MAGIC
-- MAGIC 3. <b>Views</b> - Saves the query text Used to represent intermediate results that don't need to be precomputed and persisted.
-- MAGIC
-- MAGIC When creating DLT pipelines you define your data processing steps in one of the 3 representations and DLT will handle the following:
-- MAGIC - Automatically ingesting data from source as it arrives
-- MAGIC - Resolve upstream dependencies between tables and incrementally refresh where possible.
-- MAGIC - Handle state, data quality, and more all through a declaritive framework
-- MAGIC

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC # Review Demo Data
-- MAGIC
-- MAGIC Let us review the datasets we will be using for this dataset. Our objective is to understand some of the transformations and data cleansing we'll need to do in order to make this a usable asset for the business. As we can see, the following must be done.
-- MAGIC
-- MAGIC - Clean up Data
-- MAGIC - Perform Change tracking (SCD 1 and 2)
-- MAGIC - Define a declaritive pipeline of how the data should be transformed that will process new data as it arrives
-- MAGIC
-- MAGIC This data will normally be written into Cloud Object Storage using an ingestion tool such Data Factory or FiveTran. Databricks integrates with and support a large ecosystem of ETL Tooling.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC # Declaritive Data Processing with: Delta Live Tables
-- MAGIC

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Demo Architecture
-- MAGIC
-- MAGIC In this workshop, we'll ingest and process data from our Inventory and Supply Chain Management systems using Delta Live Tables and Autoloader. For the purpose of this demo, fake data will be generated in a notebook to represent data being ingested from source into cloud object storage. In common scenarios, this portion is executed through a third-party ETL tool.
-- MAGIC
-- MAGIC
-- MAGIC - Extract Changes from our transactional/operational data stores using third-party ingestion tools, or Databricks Native ETL capabilities and save them into cloud object storage (S3, GCS, ADLS)
-- MAGIC
-- MAGIC - Leverage <b>Autoloader</b> and DLT to incrementally ingest data from cloud object storage as it arrives.
-- MAGIC
-- MAGIC - Implement cleansing, data quality, transformation, and SCD logic usind DLT Declaritive Framework.
-- MAGIC
-- MAGIC - Apply business Metadata and access Control to tables using Unity Catalog
-- MAGIC
-- MAGIC - Consume Data using Databricks SQL and visualize in PowerBI. Create Genie Room to enable consumption with natural language (for non-technical stakeholders)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 1/ Ingesting data with Autoloader into Bronze
-- MAGIC
-- MAGIC <img src="../resources/images/DE_Step1.png" width="500" style="float: right" />
-- MAGIC
-- MAGIC Our first step is to ingest the data from the cloud storage. Again, this could be from other streaming sources such as message brokers.
-- MAGIC
-- MAGIC This can be challenging for multiple reason. We have to:
-- MAGIC
-- MAGIC - operate at scale, potentially ingesting millions of small files
-- MAGIC - infer schema and json type
-- MAGIC - handle bad record with incorrect json schema
-- MAGIC - take care of schema evolution (ex: new column in the customer table)
-- MAGIC
-- MAGIC Databricks Autoloader solves all these challenges out of the box. 
-- MAGIC
-- MAGIC We will create a DLT Streaming Table using Autoloader that will automatically process new data as it arrives into cloud storage (written by third party tools, LakeFlow, ADF, etc) and write into a bronze schema. Streaming Tables are a core concept in DLT, they allow processing of growing datasets where rows are handled only once and the source is append only (Kafka, Message Brokers, Data written into Cloud Object Storage using ETL Tools, etc)

-- COMMAND ----------

-- DBTITLE 1,[EXAMPLE] Create Streaming Table and Ingest with Autoloader into Bronze

-- It is good practice to leave data unaltered as bronze to maintain fidelty (Repeat in the event of failures, auditing, etc.)

CREATE OR REFRESH STREAMING TABLE inventory_bronze
COMMENT 'Raw Data from Inventory System'
AS 
SELECT * 
FROM cloud_files("/REPLACE_ME_WITH_VOLUME/raw_data/inventory/","parquet",map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- DBTITLE 1,[EXERCISE] Ingest Supplier, Carrier, and Routes Data from Storage
-- <POPULATE THIS CELL WITH CODE TO INGEST CARRIER, SUPPLIER, AND INVENTORY DATA INTO STREAMING TABLES>

-- COMMAND ----------

-- Ingest Carrier Data

CREATE OR REFRESH STREAMING TABLE carriers_bronze
COMMENT 'Raw carrier data. Contains information about the carriers that deliver various supplies used to build our vehicles'
AS 
SELECT *
FROM cloud_files("/REPLACE_ME_WITH_VOLUME/raw_data/carriers/","parquet",map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- Ingest Supplier Data

CREATE OR REFRESH STREAMING TABLE suppliers_bronze
COMMENT 'Raw supplier description data. Contains information about the suppliers that produce the materials used in our vehicles'
AS 
SELECT * 
FROM cloud_files("/REPLACE_ME_WITH_VOLUME/raw_data/suppliers/","parquet",map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- Ingest Routes Data

CREATE OR REFRESH STREAMING TABLE routes_bronze
COMMENT 'Raw Data carrier data that delivers supply chain products'
AS 
SELECT * 
FROM cloud_files("/REPLACE_ME_WITH_VOLUME/raw_data/routes/","parquet",map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 2/ Cleanup & expectations to track data quality
-- MAGIC
-- MAGIC Next, we'll add expectations to control data quality of inventory data. To do so, we'll create another streaming table that reads from our append-only bronze data and enforces the below data quality rules using <b>Delta Live Table Expectations</b>. An expectation must be a boolean statement that always returns true or false based on some condition. You must also specify an action (warn, fail pipeline, or drop row) depending on expectation result.
-- MAGIC
-- MAGIC - `id` must never be null; drop row if found
-- MAGIC - the `carrier_id` column can be null, warn but keep row if found
-- MAGIC - when a row with a different schema from what was inferred is discoverd (captured in the `_rescued_data` column), drop row
-- MAGIC
-- MAGIC
-- MAGIC These expectations metrics are saved as technical tables and can then be re-used with Databricks SQL to track data quality over time.
-- MAGIC

-- COMMAND ----------


CREATE OR REFRESH STREAMING TABLE inventory_cleansed(
  CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_carrier_id EXPECT (carrier IS NOT NULL),
  CONSTRAINT valid_schema EXPECT (_rescued_data IS NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_supplier_id EXPECT (supplier_name IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Cleansed Inventory Data, precursor for performing SCD"
AS
SELECT id,
part_category,
SKU,
amount_in_stock,
number_of_products_using_part,
supplier_name,
sku_description,
ROUND(defective_unit_delay_costs,2) as defective_unit_delay_costs,
carrier,
ROUND(revenue_generated,2) as revenue_generated,
minimum_order_quantity,
manufacturing_site_location,
ROUND(shipping_costs,2) as shipping_costs,
ROUND(supplier_manufacturing_cost,2) as supplier_manufacturing_cost,
inspection_result,
ROUND(defect_rate,2) as defect_rate,
lead_time_days,
transportation_mode,
route,
to_date(entry_date,'dd-MM-yyyy') as entry_date,
_rescued_data
FROM STREAM(live.inventory_bronze) 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC  Complete the below cells to create cleansed streaming tables for each of the following dimensions
-- MAGIC
-- MAGIC ### Supplier
-- MAGIC - `supplier_name` must not be null. If it drop the row
-- MAGIC - cast `row_process_date` as date column as timestamp using CAST
-- MAGIC ### Carrier
-- MAGIC - `shipping_carriers` must not be null. if it is drop row
-- MAGIC - cast `row_process_date` as date column as timestamp using CAST
-- MAGIC ### Routes
-- MAGIC - `routes` must not be null. if it is drop row
-- MAGIC - cast `row_process_date` as date column as timestamp using CAST

-- COMMAND ----------


CREATE OR REFRESH STREAMING TABLE suppliers_cleansed(
supplier_name STRING NOT NULL,
payment_settlement_days BIGINT,
last_verified_procurement_comment STRING,
row_process_date TIMESTAMP,
_rescued_data STRING
CONSTRAINT supplier_id EXPECT (supplier_name is NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'cleansed supplier table, prepared for SCD type 2'
AS
SELECT supplier_name,
payment_settlement_days,
last_verified_procurement_comment,
CAST(row_process_date AS TIMESTAMP) row_process_date,
_rescued_data
FROM STREAM(live.suppliers_bronze)

-- COMMAND ----------


CREATE OR REFRESH STREAMING TABLE carriers_cleansed(
  shipping_carriers STRING NOT NULL,
  name STRING,
  row_process_date TIMESTAMP,
  _rescued_data STRING,
  CONSTRAINT carrier_name EXPECT (shipping_carriers IS NOT NULL) ON VIOLATION DROP ROW
)
AS
SELECT shipping_carriers,
name,
CAST(row_process_date AS TIMESTAMP) row_process_date,
_rescued_data
FROM STREAM(live.carriers_bronze)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE routes_cleansed(
routes STRING NOT NULL,
origin string,
destination string,
row_process_date TIMESTAMP,
_rescued_data string,
CONSTRAINT valid_routes EXPECT (routes IS NOT NULL) ON VIOLATION DROP ROW
)
AS 
SELECT routes,
origin,
destination,
CAST(row_process_date as TIMESTAMP) row_process_date
FROM STREAM(live.routes_bronze)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 3/ Create SCD Tables using APPLY CHANGES
-- MAGIC
-- MAGIC This table will contain a history of how records from our suppliers, routes, and carriers streaming tables have changed overtime. Commonly known as a <b>Slowly Changing Dimension</b>
-- MAGIC
-- MAGIC Implementing this is non-trivial, previously, developers had to consider issues such as deduplication, and out-of-order data.
-- MAGIC
-- MAGIC With DLT, these difficulties are abstracted away using the `APPLY CHANGE` operation. This leverages delta formats change data feed (CDF) to track row-level changes from underlying streaming tables. note: since you cannot read CDF of Materialized views, they cannot be used as a source. 

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE suppliers_scd1

-- COMMAND ----------


APPLY CHANGES INTO live.suppliers_scd1
FROM STREAM(live.suppliers_cleansed)
KEYS (supplier_name)
--If row operation data is available, such as fields indicating if the row was "APPENDED","INSERTED", "DELETED" can leverage APPLY AS DELETE
SEQUENCE BY (row_process_date) --any auto-incrementing date or ID that can be used to identify order of events, or a timestamp
COLUMNS * EXCEPT(row_process_date)
STORED AS
  SCD TYPE 1


-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE carriers_scd1

-- COMMAND ----------


APPLY CHANGES INTO live.carriers_scd1
FROM STREAM(live.carriers_cleansed)
KEYS (shipping_carriers)
SEQUENCE BY (row_process_date)
COLUMNS * EXCEPT (row_process_date)
STORED AS 
  SCD TYPE 1

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE routes_scd2

-- COMMAND ----------


APPLY CHANGES INTO live.routes_scd2
FROM STREAM(live.routes_cleansed)
KEYS (routes)
SEQUENCE BY (row_process_date)
COLUMNS * EXCEPT (row_process_date)
STORED AS 
  SCD TYPE 2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4/ Create Curated View
-- MAGIC
-- MAGIC Views can be used to hold queries that answer a specific business question. Lets create the following view:
-- MAGIC
-- MAGIC - The average Defect Rate across all parts that originated from Taiwan.
-- MAGIC - Count of inspection results, split by part category
-- MAGIC - Number of parts are in stock 
-- MAGIC
-- MAGIC If we would like to persist the result of the view, we can leveraged `materialized views`

-- COMMAND ----------

CREATE OR REPLACE MATERIALIZED VIEW supplier_defect_rates
AS
SELECT 
AVG(defect_rate*100) defect_rate
FROM live.inventory_cleansed i
JOIN live.routes_scd2 r
on i.route = r.routes
where r.origin = 'Taiwan'