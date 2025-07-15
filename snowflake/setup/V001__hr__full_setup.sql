-- V001__hr__full_setup.sql
-- Full DDL for the HR schema

-- 1. Create schema
CREATE SCHEMA IF NOT EXISTS hr;

-- 2. Create warehouse
CREATE WAREHOUSE IF NOT EXISTS hr_wh
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 120
  AUTO_RESUME = TRUE;

-- 3. Role & grants
CREATE ROLE IF NOT EXISTS hr_analyst;
GRANT USAGE ON WAREHOUSE hr_wh TO ROLE hr_analyst;
GRANT USAGE ON DATABASE TPCH TO ROLE hr_analyst;
GRANT USAGE ON SCHEMA TPCH.hr TO ROLE hr_analyst;

-- 4. Create tables
CREATE OR REPLACE TABLE hr.employees (
  employee_id INT,
  first_name STRING,
  last_name STRING,
  hire_date DATE,
  department STRING
);
CREATE OR REPLACE TABLE hr.employees2 (
  employee_id INT,
  first_name STRING,
  last_name STRING,
  hire_date DATE,
  department STRING
);
CREATE OR REPLACE TABLE hr.salaries (
  employee_id INT,
  salary NUMERIC(10,2),
  from_date DATE,
  to_date DATE
);

-- 5. Create view
CREATE OR REPLACE VIEW hr.v_current_salaries AS
SELECT e.employee_id, e.first_name, e.last_name, s.salary
FROM hr.employees e
JOIN hr.salaries s
  ON e.employee_id = s.employee_id
WHERE s.to_date IS NULL;

-- 6. Sequence
CREATE OR REPLACE SEQUENCE hr.emp_seq START = 5000 INCREMENT = 1;

-- 7. File format & stage
CREATE OR REPLACE FILE FORMAT hr.json_ff
  TYPE = 'JSON';


-- 8. Grants
GRANT SELECT ON ALL TABLES IN SCHEMA hr TO ROLE hr_analyst;
GRANT SELECT ON ALL VIEWS IN SCHEMA hr TO ROLE hr_analyst;
GRANT USAGE ON SEQUENCE hr.emp_seq TO ROLE hr_analyst;
GRANT USAGE ON FILE FORMAT hr.json_ff TO ROLE hr_analyst;
GRANT USAGE ON STAGE hr.employee_stage TO ROLE hr_analyst;
