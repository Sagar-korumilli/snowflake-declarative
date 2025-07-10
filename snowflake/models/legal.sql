CREATE OR REPLACE TABLE legal.example_table (
    id INT,
    name STRING
);

CREATE OR REPLACE VIEW legal.example_view AS
SELECT * FROM legal.example_table;
