-- snowflake/migration/admin/V002__alter_table_example.sql

ALTER TABLE admin.example_table
  ADD COLUMN IF NOT EXISTS created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP;
