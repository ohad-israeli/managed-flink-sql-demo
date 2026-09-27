-- Step 5: seed sample clickstream events. c1/c3 belong to user 101 (Alice),
-- c2 to user 102 (Ben), c4 to user 103 (Carla) -- this is what makes the
-- join result in step 6 verifiable by inspection.
INSERT INTO `fresh-catalog`.`fresh-db`.clicks VALUES
  ('c1', 101, '/products/shoes', 'Mozilla/5.0', 42),
  ('c2', 102, '/products/hats', 'Chrome/120', 17),
  ('c3', 101, '/cart', 'Mozilla/5.0', 95),
  ('c4', 103, '/products/bags', 'Safari/17', 8);
