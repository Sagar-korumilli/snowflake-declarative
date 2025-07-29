CREATE OR REPLACE TABLE sales.order_items (
  order_id INT,
  line_item_id INT,
  product_id INT,
  quantity INT,
  price NUMBER(10,2)
);
