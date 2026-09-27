-- Step 3: output table the enrichment job writes to.
-- Note: `CREATE TABLE ... AS SELECT` is NOT supported against a Kafka
-- catalog in CMF ("CREATE TABLE AS is not supported on Kafka catalog") --
-- that's why this is a plain CREATE TABLE, with the join done separately
-- as an INSERT INTO ... SELECT in step 6.
CREATE TABLE `fresh-catalog`.`fresh-db`.customer_click_activity (
  click_id STRING,
  user_id INT,
  customer_name STRING,
  customer_email STRING,
  url STRING,
  user_agent STRING,
  view_time INT
);
