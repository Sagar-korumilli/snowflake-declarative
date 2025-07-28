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
