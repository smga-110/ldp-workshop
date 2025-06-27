# Databricks notebook source
dbutils.widgets.text("hostName", "", "Host Name")

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import current_timestamp, date_format

currentDate = datetime.now().strftime("%Y")
currentUser = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
namePart = currentUser.split('@')[0]
firstName, lastName = namePart.split('.')
userUID = f"{firstName}_{lastName[:3]}_ws{currentDate}"
print(f"--> The following userUID will be use to create your schema: {userUID}")

catalog = "demo_catalog"
schema = userUID
volume_name = "raw_data"
volume_folder =  f"/Volumes/{catalog}/{schema}/{volume_name}"
print (f"-> New data will be written into this directory: {volume_folder}")

# COMMAND ----------

# DBTITLE 1,Incremental Change Data
data_routes = [("route_A", "Thailand","Vancouver"),("route_E","Montreal","Spain")]
data_carriers = [("carrier_X","WorldWide Carriers")]

data_suppliers = [("supplier_1",30,"Great Quality with reasonable MoQ and low defect rates. Highly recommended for sources for high-value products where theres substantial impact to the business when defects are detected"),("supplier_2",60,"Parts are often delivered on-time, expected number of defects. No complaints from shop floor team"),("supplier_3",20,"may consider placing this suppler on Do Not Order list. Repeated late shippments and quality issues. Terrible and unresponsive customer service"),("supplier_4",45,"No particular recommendation. meets deliver dates and contract equipments as expected")]


df_routes = spark.createDataFrame(data_routes,["routes","origin","destination"]).withColumn("row_process_date",date_format(current_timestamp(), "yyyy-MM-dd HH:mm"))
df_carriers = spark.createDataFrame(data_carriers,["shipping_carriers","name"]).withColumn("row_process_date",date_format(current_timestamp(), "yyyy-MM-dd HH:mm"))
df_suppliers = spark.createDataFrame(data_suppliers,["supplier_name","payment_settlement_days","last_verified_procurement_comment"]).withColumn("row_process_date",date_format(current_timestamp(), "yyyy-MM-dd HH:mm"))


# COMMAND ----------

df_routes.write.mode("append").format("parquet").save(f"{volume_folder}/routes")
df_carriers.write.mode("append").format("parquet").save(f"{volume_folder}/carriers")
df_suppliers.write.mode("append").format("parquet").save(f"{volume_folder}/suppliers")


# COMMAND ----------

