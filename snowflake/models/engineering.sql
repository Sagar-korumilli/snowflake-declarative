CREATE OR REPLACE TABLE engineering.example_table (
    id INT,
    name STRING
);

CREATE OR REPLACE VIEW engineering.example_view AS
SELECT * FROM engineering.example_table;
