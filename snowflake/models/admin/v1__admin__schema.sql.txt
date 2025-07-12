CREATE OR REPLACE TABLE admin.example_table (
    id INT,
    name STRING
);

CREATE OR REPLACE VIEW admin.example_view AS
SELECT * FROM admin.example_table;