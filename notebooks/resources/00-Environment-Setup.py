# Databricks notebook source
# DBTITLE 1,Faker Library used to Generate Data
# MAGIC %pip install Faker

# COMMAND ----------

# DBTITLE 1,Create unique ID for User Catalogs
from datetime import datetime

currentDate = datetime.now().strftime("%Y")
currentUser = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
namePart = currentUser.split('@')[0]
firstName, lastName = namePart.split('.')
userUID = f"{firstName}_{lastName[:3]}_ws{currentDate}"
print(f"--> The following userUID will be use to create your catalog: {userUID}")

# COMMAND ----------

# DBTITLE 1,Create Catalog, Schemas, and Volumes

catalog = "demo_catalog"
schema = userUID
volume_name = "raw_data"

print(f"--> Set current catalog context to {catalog}")
spark.sql(f'USE CATALOG `{catalog}`')
print(f"--> Create workshop Schema {schema}, DLT Tables will be stored here")
spark.sql(f'CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`')
print(f"--> Set context to {schema} and create volume {volume_name} to hold demo data")
spark.sql(f'USE SCHEMA `{schema}`')
spark.sql(f'CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume_name}`')
volume_folder =  f"/Volumes/{catalog}/{schema}/{volume_name}"

# COMMAND ----------

print ("--> Generating Fake Routes, Carriers, Suppliers, and Inventory Data ")

from pyspark.sql.functions import current_timestamp, date_format

data_routes = [("route_A", "Singapore","Vancouver"),("route_B","Taiwan","Los Angeles"), ("route_C","Austria","Halifax"),("route_D","Germany","Tokyo")]
data_carriers = [("carrier_A","ONE (Ocean Network Express)"), ("carrier_B","MSC (Mediterranean Shipping Company)"), ("carrier_C","Maersk"), ("carrier_D","Evergreen Shipping")]

data_suppliers = [("supplier_1",30,"Great Quality with reasonable MoQ and low defect rates. Highly recommended for sources for high-value products where theres substantial impact to the business when defects are detected"),("supplier_2",60,"Parts are often delivered on-time, expected number of defects. No complaints from shop floor team"),("supplier_3",20,"may consider placing this suppler on Do Not Order list. Repeated late shippments and quality issues. Terrible and unresponsive customer service"),("supplier_4",45,"No particular recommendation. meets deliver dates and contract equipments as expected")]


df_routes = spark.createDataFrame(data_routes,["routes","origin","destination"]).withColumn("row_process_date",date_format(current_timestamp(), "yyyy-MM-dd HH:mm"))
df_carriers = spark.createDataFrame(data_carriers,["shipping_carriers","name"]).withColumn("row_process_date",date_format(current_timestamp(), "yyyy-MM-dd HH:mm"))
df_suppliers = spark.createDataFrame(data_suppliers,["supplier_name","payment_settlement_days","last_verified_procurement_comment"]).withColumn("row_process_date",date_format(current_timestamp(), "yyyy-MM-dd HH:mm"))


df_routes.write.mode("overwrite").format("parquet").save(f"{volume_folder}/routes")
df_carriers.write.mode("overwrite").format("parquet").save(f"{volume_folder}/carriers")
df_suppliers.write.mode("overwrite").format("parquet").save(f"{volume_folder}/suppliers")


# COMMAND ----------

try:
  dbutils.fs.ls(volume_folder+"/inventory")
  dbutils.fs.ls(volume_folder+"/routes")
  dbutils.fs.ls(volume_folder+"/carriers")
  dbutils.fs.ls(volume_folder+"/suppliers")
except:  
  print(f"folder doesn't exists, generating the data under {volume_folder}...")
  from pyspark.sql import functions as F
  from faker import Faker
  from collections import OrderedDict 
  import uuid
  fake = Faker()
  import random


  part_category = OrderedDict([
    ("electrical_system", 0.1),
    ("fasteners", 0.1),
    ("engine_components", 0.1),
    ("suspension", 0.1),
    ("braking_system", 0.1),
    ("transmission", 0.1),
    ("body_structure", 0.1),
    ("interior", 0.1),
    ("fuel_system", 0.1),
    (None, 0.1)
  ])

  sku_description = OrderedDict([
    ("screw", 0.1 ),
    ("lubricant", 0.1),
    ("cap", 0.1),
    ("plastic", 0.1),
    ("washer", 0.1),
    ("nut",0.1),
    ("bolt",0.1),
    ("handle",0.1),
    ("rubber",0.1),
    ("miscellanuous",0.1)
  ])

  route_names = OrderedDict([("route_A", 0.25), ("route_B", 0.25), ("route_C", 0.25), ("route_D", 0.25)])
  
  supplier_names = OrderedDict([("supplier_1", 0.25), ("supplier_2", 0.25), ("supplier_3", 0.25), ("supplier_4", 0.25)])

  carrier_names = OrderedDict([("carrier_A", 0.25), ("carrier_B", 0.25), ("carrier_C", 0.3), ("carrier_D", 0.1),(None,0.1)])

  fake_partCategory = F.udf(lambda: fake.random_elements(elements=part_category, length=1)[0])
  fake_supplierNames = F.udf(lambda: fake.random_elements(elements=supplier_names, length=1)[0])
  fake_skuDescription = F.udf(lambda: fake.random_elements(elements=sku_description, length=1)[0])
  fake_carrierNames = F.udf(lambda: fake.random_elements(elements=carrier_names, length=1)[0])

  df_supplier_data = spark.range(0, 100000).repartition(100)
  df_supplier_data = df_supplier_data.withColumn("part_category", fake_partCategory())
  df_supplier_data = df_supplier_data.withColumn("SKU", F.monotonically_increasing_id())
  df_supplier_data = df_supplier_data.withColumn("amount_in_stock",(F.rand() * 401 + 100).cast("int"))
  df_supplier_data = df_supplier_data.withColumn("number_of_products_using_part", (F.rand() * 30 + 10).cast("int"))
  df_supplier_data = df_supplier_data.withColumn("supplier_name", fake_supplierNames())
  df_supplier_data = df_supplier_data.withColumn("sku_description", fake_skuDescription())
  df_supplier_data = df_supplier_data.withColumn("defective_unit_delay_costs", 
    (F.rand() * 20 + 20))
  df_supplier_data = df_supplier_data.withColumn("carrier", fake_carrierNames())
  df_supplier_data = df_supplier_data.withColumn("revenue_generated", F.rand() * 1200 + 600).withColumn(
    "minimum_order_quantity", 
    F.expr("array(25, 50, 100, 200, 300)[int(rand() * 5)]")
).withColumn(
    "manufacturing_site_location", 
    F.expr("array('Mumbai', 'Taiwan', 'Singapore', 'Germany', 'Austria', 'Japan')[int(rand() * 6)]")
).withColumn(
    "shipping_costs", 
    F.rand() * 150 + 50
).withColumn(
    "supplier_manufacturing_cost", 
    F.rand() * 20 + 10
).withColumn(
    "inspection_result", 
    F.expr("array('Passed', 'Failed', 'Conditional Pass', 'Conditional Fail')[int(rand() * 4)]")
).withColumn(
    "defect_rate", 
    F.rand() * 0.02
).withColumn(
    "lead_time_days", 
    F.expr("int(rand() * 21 + 10)")
).withColumn(
    "transportation_mode", 
    F.expr("array('Road', 'Air', 'Sea')[int(rand() * 3)]")
).withColumn(
  "route",
  F.expr("array('route_A', 'route_B', 'route_C', 'route_D')[int(rand() * 4)]")
).withColumn(
  "entry_date",F.expr("array('01-01-2020', '01-04-2020', '01-08-2020', '01-11-2020','01-01-2021', '01-04-2021', '01-08-2021', '01-11-2021','01-01-2022', '01-04-2022', '01-08-2022', '01-11-2022', '01-01-2023', '01-04-2023')[int(rand() * 14)]")
  )

  df_supplier_data.write.mode("overwrite").format("parquet").save(f"{volume_folder}/inventory")



# COMMAND ----------

print(f"--> data generated under, Please copy and record this directory somewhere {volume_folder}")
dbutils.fs.ls(volume_folder)