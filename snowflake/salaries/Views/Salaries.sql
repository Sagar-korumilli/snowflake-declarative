CREATE OR REPLACE VIEW salaries.v_salaries AS
SELECT
    employee_id,
    salary,
    from_date
FROM
    salaries.salaries;
