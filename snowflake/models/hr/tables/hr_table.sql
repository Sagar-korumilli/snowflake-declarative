CREATE OR REPLACE TABLE hr.example_table (
    id INT,
    name STRING
);

CREATE OR REPLACE VIEW hr.example_view AS
SELECT * FROM hr.example_table;
