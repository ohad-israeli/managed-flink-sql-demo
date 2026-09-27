-- Step 1: source table for the clickstream side of the join.
-- CREATE TABLE against a Kafka catalog auto-provisions the underlying Kafka
-- topic and its Avro schema in Schema Registry -- no manual topic/schema setup.
CREATE TABLE `fresh-catalog`.`fresh-db`.clicks (
  click_id STRING,
  user_id INT,
  url STRING,
  user_agent STRING,
  view_time INT
);
