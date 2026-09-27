-- Step 4: seed sample customers. A bounded INSERT INTO ... VALUES runs to
-- completion (status COMPLETED), unlike the unbounded streaming join in
-- step 6 which stays RUNNING.
INSERT INTO `fresh-catalog`.`fresh-db`.customers VALUES
  (101, 'Alice Nguyen', 'alice@example.com'),
  (102, 'Ben Ortiz', 'ben@example.com'),
  (103, 'Carla Diaz', 'carla@example.com');
