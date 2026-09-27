-- Step 6: the actual enrichment job. Unbounded streaming INSERT INTO ...
-- SELECT with a regular (non-temporal) join -- stays RUNNING continuously,
-- emitting a new row to customer_click_activity for every click that has a
-- matching customer.
INSERT INTO `fresh-catalog`.`fresh-db`.customer_click_activity
SELECT
  c.click_id,
  c.user_id,
  cu.customer_name,
  cu.customer_email,
  c.url,
  c.user_agent,
  c.view_time
FROM `fresh-catalog`.`fresh-db`.clicks AS c
JOIN `fresh-catalog`.`fresh-db`.customers AS cu
  ON c.user_id = cu.user_id;
