CREATE OR REPLACE TABLE sales.example_table (
    id INT,
    name STRING
);

CREATE OR REPLACE VIEW sales.example_view AS
SELECT * FROM sales.example_table;
