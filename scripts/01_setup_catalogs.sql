-- =============================================================================
-- CATALOG SETUP: one catalog per workspace
--
--   Workspace        Catalog to create
--   ---------------  -----------------
--   dbx-cicd-dev     dev_catalog
--   dbx-cicd-uat     uat_catalog
--   dbx-cicd-prod    prod_catalog
--
-- STEP 1 (UI, in EACH workspace, create only that workspace's catalog):
--   Catalog (left menu) > "+" > Create catalog
--     Name: dev_catalog (or uat_catalog / prod_catalog), type Standard
--     Tick "Use default storage" > Create
--   (Plain CREATE CATALOG in SQL fails with "Metastore storage root URL does
--    not exist". The UI option with default storage works.)
--
-- STEP 2 (SQL): NOT needed if GitHub deploys with YOUR PAT. You created the
--   catalog, so you already own it.
--   Needed ONLY if you deploy with a service principal. Then run the matching
--   GRANT below in the matching workspace (SQL Editor, serverless warehouse).
--   Replace <SP_APPLICATION_ID> with that workspace's service principal ID.
-- =============================================================================


-- ----- Run in the DEV workspace -------------------------------------------
-- GRANT USE CATALOG, CREATE SCHEMA, USE SCHEMA, CREATE TABLE, SELECT, MODIFY
--   ON CATALOG dev_catalog  TO `<SP_APPLICATION_ID>`;

-- ----- Run in the UAT workspace -------------------------------------------
-- GRANT USE CATALOG, CREATE SCHEMA, USE SCHEMA, CREATE TABLE, SELECT, MODIFY
--   ON CATALOG uat_catalog  TO `<SP_APPLICATION_ID>`;

-- ----- Run in the PROD workspace ------------------------------------------
-- GRANT USE CATALOG, CREATE SCHEMA, USE SCHEMA, CREATE TABLE, SELECT, MODIFY
--   ON CATALOG prod_catalog TO `<SP_APPLICATION_ID>`;


-- -----------------------------------------------------------------------------
-- Check (run in each workspace): you should see that workspace's catalog
-- -----------------------------------------------------------------------------
SHOW CATALOGS LIKE '*_catalog';
