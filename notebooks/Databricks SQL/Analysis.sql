-- Databricks SQL provides industry leading Data Warehousing compute, with one of the best cost-performance ratios in the industry
-- SQL Warehouses are fully serverless, enabling independantly scaling of compute and storage to meet your business needs
-- Databricks SQL allows for seamless usage of AI models for common use-cases such as sentiment analysis and forecasting through AI Functions
-- Works seamlessly with PowerBI
-- Databricks is built enabling to consume data from any source, regardless of where it resides through Unity Catalog. We can combine data across Catalogs, through Data Shares, and through On-prem data sources using Lakehouse Federation.

-- Demo 1: Showcase how we can use Databricks SQL to query, combine, and visualize data
-- Lets start with a simple example of combining data from two different tables; inventory and routes tables

select i.*,r.origin,r.destination
from moe_abd_steyrws2024.bronze.inventory_cleansed i
join moe_abd_steyrws2024.bronze.routes_scd2 r
on i.route = r.routes
and r.`__END_AT` is null

-- Databricks SQL Supports ANSI SQL, common capabilities like Aggregating, Group Bys, Window Functions and more are supported
select supplier_name, avg(shipping_costs) as avg_shipping_cost
from moe_abd_steyrws2024.bronze.inventory_cleansed
group by supplier_name

-- Lets try something a little more advanced using the assistant; count how many parts in each part category originated from Taiwan
select part_category, count(*) as part_count
from moe_abd_steyrws2024.bronze.inventory_cleansed
where manufacturing_site_location = 'Taiwan'
group by part_category


-- We can also leverage Databricks AI Functions to help us with sentiment analysis. Previously, enabling this capability
-- would require expertise with Machine Learning, NLP, and Model Deployment. Now, non technical uses can leverage the power
-- of Databricks SQL to do this. Lets use the ai_analyze_sentiment to see the sentiment from Procurement teams about current suppliers:
select last_verified_procurement_comment,ai_analyze_sentiment(last_verified_procurement_comment)
from moe_abd_steyrws2024.bronze.suppliers_cleansed