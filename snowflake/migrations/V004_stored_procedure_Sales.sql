CREATE OR REPLACE PROCEDURE get_customer_count_by_country(country_name VARCHAR)
RETURNS NUMBER
LANGUAGE SQL
AS
$$
DECLARE
    customer_count NUMBER;
    sql_command VARCHAR;
BEGIN
    -- Construct the SQL query dynamically
    sql_command := 'SELECT COUNT(*) FROM sales.orders WHERE customer_id IN (SELECT customer_id FROM sales.customers WHERE country = ''' || :country_name || ''');';

    -- Execute the dynamic SQL and store the result
    EXECUTE IMMEDIATE :sql_command INTO customer_count;

    -- Return the count
    RETURN customer_count;

EXCEPTION
    WHEN OTHER THEN
        -- Log any errors that occur during execution
        CALL SYSTEM$LOG('ERROR', 'Error in get_customer_count_by_country: ' || SQLERRM);
        RETURN -1; -- Return an error code or re-raise the exception
END;
$$;

-- Example of how to call the stored procedure:
-- CALL get_customer_count_by_country('USA');
-- CALL get_customer_count_by_country('Canada');
