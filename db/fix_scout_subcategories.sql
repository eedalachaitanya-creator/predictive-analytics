-- Re-add scout agent sub_categories with CORRECT category_id mappings
-- Based on the fixed categories table:
--   1 = Clothing & Apparel
--   2 = Automotive
--   3 = Baby & Kids
--   4 = Electronics
--   5 = Grocery
--   6 = Health & Beauty
--   7 = Home & Garden
--   8 = Office Supplies
--   9 = Sports & Outdoors
--  10 = Toys & Games

INSERT INTO sub_categories (sub_category_id, sub_category_name, category_id) VALUES
  (101, 'Smartphones',   4),   -- Electronics
  (102, 'Laptops',       4),   -- Electronics
  (201, 'Men''s Wear',   1),   -- Clothing & Apparel
  (202, 'Women''s Wear', 1),   -- Clothing & Apparel
  (301, 'Snacks',        5),   -- Grocery
  (302, 'Beverages',     5),   -- Grocery
  (401, 'Furniture',     7),   -- Home & Garden
  (501, 'Fitness',       9)    -- Sports & Outdoors
ON CONFLICT (sub_category_id) DO NOTHING;
