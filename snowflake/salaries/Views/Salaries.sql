CREATE OR REPLACE VIEW salaries.v_salaries AS
SELECT
    employee_id,
    from_date
FROM
    salaries.salaries;
