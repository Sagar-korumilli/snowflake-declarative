
-- Example SQL to test SQLFluff
create or replace table customer_orders (
    order_id integer,
    order_date date,
    customer_id integer   -- missing comma will cause a lint error!
    order_total float
);

insert into customer_orders (order_id, order_date, customer_id, order_total)
values (1, '2024-07-01', 100, 150.50);
