-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS admin;

-- Create table
CREATE OR REPLACE TABLE admin.example_table (
    id INT,
    name STRING
);

-- Create view
CREATE OR REPLACE VIEW admin.example_view AS
SELECT * FROM admin.example_table;
