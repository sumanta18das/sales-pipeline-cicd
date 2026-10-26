-- =============================================================================
-- CLEANUP after the session (removes ALL demo data).
--
-- Step 1: remove the jobs and deployed files (do this FIRST):
--   - GitHub > Actions > "Destroy environment" > Run workflow > prod (approve it)
--   - repeat for uat and dev
--
-- Step 2: run the matching line in the SQL Editor of EACH workspace:
-- =============================================================================

-- In the DEV workspace:
DROP CATALOG IF EXISTS dev_catalog CASCADE;

-- In the UAT workspace:
-- DROP CATALOG IF EXISTS uat_catalog CASCADE;

-- In the PROD workspace:
-- DROP CATALOG IF EXISTS prod_catalog CASCADE;

-- Step 3 (Azure portal): delete the resource group to remove all 3 workspaces.
