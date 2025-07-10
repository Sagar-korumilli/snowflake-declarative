CREATE OR REPLACE TABLE logistics.example_table (
    id INT,
    name STRING
);

CREATE OR REPLACE VIEW logistics.example_view AS
SELECT * FROM logistics.example_table;
