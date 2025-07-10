CREATE OR REPLACE TABLE marketing.example_table (
    id INT,
    name STRING
);

CREATE OR REPLACE VIEW marketing.example_view AS
SELECT * FROM marketing.example_table;
