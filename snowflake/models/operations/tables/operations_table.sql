CREATE OR REPLACE TABLE operations.example_table (
    id INT,
    name STRING
);

CREATE OR REPLACE VIEW operations.example_view AS
SELECT * FROM operations.example_table;
