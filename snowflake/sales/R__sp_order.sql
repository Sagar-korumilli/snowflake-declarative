CREATE OR REPLACE PROCEDURE DEVOPS.SALES.INSERT_NEW_ORDER(
    ORDER_ID NUMBER,
    CUSTOMER_ID NUMBER,
    ORDER_DATE DATE,
    TOTAL_AMOUNT NUMBER(10, 2),
    ORDER_NAME VARCHAR(20)
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Insert a hardcoded order record into the table.
    -- The parameters passed to the procedure are no longer used.
    INSERT INTO DEVOPS.SALES.ORDERS (
        ORDER_ID,
        CUSTOMER_ID,
        ORDER_DATE,
        TOTAL_AMOUNT,
        NAME
    ) VALUES (
        1004,
        201,
        '2025-07-25',
        150.50,
        'Laptop'
    );
    -- Return a success message.
    RETURN 'New order inserted successfully.';
END;
$$;
