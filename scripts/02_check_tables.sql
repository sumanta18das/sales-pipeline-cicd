-- =============================================================================
-- Use during the demo to SHOW that the same code produced the same tables
-- in every workspace.
--
-- Run in the SQL Editor of EACH workspace (serverless SQL warehouse).
-- Change ONLY the first line:
--   DEV  workspace:  USE CATALOG dev_catalog;
--   UAT  workspace:  USE CATALOG uat_catalog;
--   PROD workspace:  USE CATALOG prod_catalog;
-- =============================================================================

USE CATALOG prod_catalog;
USE SCHEMA sales;

-- 1. Row counts per layer. Expected in every workspace: bronze 31, silver 26, gold 9
SELECT 'bronze' AS layer, COUNT(*) AS row_count FROM bronze_orders
UNION ALL SELECT 'silver', COUNT(*) FROM silver_orders
UNION ALL SELECT 'gold',   COUNT(*) FROM gold_daily_sales;

-- 2. The business output
SELECT * FROM gold_daily_sales ORDER BY order_date, category;

-- 3. When and by whom the table was written (should be the CI/CD job run,
--    not someone typing in a notebook)
DESCRIBE HISTORY gold_daily_sales;

-- 4. Isolation check: try to read ANOTHER environment's catalog from here.
--    Example from the DEV workspace:  SELECT * FROM prod_catalog.sales.gold_daily_sales;
--    Expected: error. Each catalog is bound to its own workspace.
