-- V001__sales__full_setup.sql
-- Full DDL for the SALES schema

-- 1. Create schema
CREATE SCHEMA IF NOT EXISTS sales;

-- 2. Create warehouse (assumes ACCOUNTADMIN or sufficient privileges)
CREATE WAREHOUSE IF NOT EXISTS sales_wh
  WITH WAREHOUSE_SIZE = 'XSMALL'
  WAREHOUSE_TYPE = 'STANDARD'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

-- 3. Create role and grants
CREATE ROLE IF NOT EXISTS sales_analyst;
GRANT USAGE ON WAREHOUSE sales_wh TO ROLE sales_analyst;
GRANT USAGE ON DATABASE TPCH TO ROLE sales_analyst;
GRANT USAGE ON SCHEMA TPCH.sales TO ROLE sales_analyst;

-- 4. Create tables
CREATE OR REPLACE TABLE sales.orders (
  order_id INT,
  customer_id INT,
  order_date DATE,
  total_amount NUMBER(10,2)
);
CREATE OR REPLACE TABLE sales.orders2 (
  order_id INT,
  customer_id INT,
  order_date DATE,
  total_amount NUMBER(10,2)
);
CREATE OR REPLACE TABLE sales.order_items (
  order_id INT,
  line_item_id INT,
  product_id INT,
  quantity INT,
  price NUMBER(10,2)
);

-- 5. Create a view
CREATE OR REPLACE VIEW sales.v_order_summary AS
SELECT
  o.order_id,
  o.order_date,
  SUM(i.quantity * i.price) AS computed_total
FROM sales.orders o
JOIN sales.order_items i
  ON o.order_id = i.order_id
GROUP BY o.order_id, o.order_date;

-- 6. Sequence
CREATE OR REPLACE SEQUENCE sales.order_seq START = 1000 INCREMENT = 1;

-- 7. File format & stage
CREATE OR REPLACE FILE FORMAT sales.csv_ff
  TYPE = ‘CSV’
  FIELD_DELIMITER = ‘,’
  SKIP_HEADER = 1;

CREATE OR REPLACE STAGE sales.orders_stage
  URL = 's3://my-bucket/sales/'
  FILE_FORMAT = sales.csv_ff
  CREDENTIALS = (AWS_KEY_ID='{{AWS_KEY}}' AWS_SECRET_KEY='{{AWS_SECRET}}');

-- 8. Grants on objects
GRANT SELECT ON ALL TABLES IN SCHEMA sales TO ROLE sales_analyst;
GRANT SELECT ON ALL VIEWS IN SCHEMA sales TO ROLE sales_analyst;
GRANT USAGE ON SEQUENCE sales.order_seq TO ROLE sales_analyst;
GRANT USAGE ON FILE FORMAT sales.csv_ff TO ROLE sales_analyst;
GRANT USAGE ON STAGE sales.orders_stage TO ROLE sales_analyst;
