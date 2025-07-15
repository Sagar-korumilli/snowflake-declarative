-- V003__hr__add_phone_number_to_employees.sql
-- Adds a PHONE_NUMBER column to the hr.employees table.

ALTER TABLE hr.employees
ADD COLUMN PHONE_NUMBER VARCHAR(20);

-- You can add more statements here if part of the same logical change.
-- For example, updating existing data or granting permissions on the new column:
-- UPDATE hr.employees SET PHONE_NUMBER = 'N/A' WHERE PHONE_NUMBER IS NULL;
-- GRANT SELECT ON COLUMN hr.employees.PHONE_NUMBER TO ROLE hr_analyst;
