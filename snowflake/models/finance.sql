CREATE OR REPLACE TABLE finance.example_table (
    id INT,
    name STRING
);

CREATE OR REPLACE VIEW finance.example_view AS
SELECT * FROM finance.example_table;
