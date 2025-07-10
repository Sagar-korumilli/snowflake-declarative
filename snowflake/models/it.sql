CREATE OR REPLACE TABLE it.example_table (
    id INT,
    name STRING
);

CREATE OR REPLACE VIEW it.example_view AS
SELECT * FROM it.example_table;
