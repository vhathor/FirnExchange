/*==============================================================================
  FirnExchange - User Role and Privileges Setup Script
  
  Purpose: Create a role for FirnExchange users with appropriate privileges
  Version: 1.0
  
  Usage:
    Run this script as SECURITYADMIN or ACCOUNTADMIN after deploying FirnExchange
    
  Description:
    This script creates a role (FIRNEXCHANGE_USER) that grants users the minimum
    privileges needed to use the FirnExchange Streamlit application for data
    migration operations.
    
  What users can do with this role:
    ✓ Access and use the FirnExchange Streamlit application
    ✓ View and select databases, schemas, tables, and stages
    ✓ Create and manage migration tracking tables (log tables)
    ✓ Export data from tables to external stages
    ✓ Import data from external stages to Iceberg tables
    ✓ Monitor migration progress
    
  What users CANNOT do:
    ✗ Drop or alter source tables
    ✗ Modify application code or deployment
    ✗ Grant privileges to other users
    ✗ Access data outside granted databases/schemas
==============================================================================*/

-- Execute as SECURITYADMIN or ACCOUNTADMIN
USE ROLE SECURITYADMIN;

--------------------------------------------------------------------------------
-- STEP 1: Create FirnExchange User Role
--------------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS FIRNEXCHANGE_USER
    COMMENT = 'Role for users who need to use FirnExchange data migration tool';

-- Optional: Create a more restricted role for read-only monitoring
CREATE ROLE IF NOT EXISTS FIRNEXCHANGE_VIEWER
    COMMENT = 'Role for users who only need to view FirnExchange migration status';

--------------------------------------------------------------------------------
-- STEP 2: Grant Access to Streamlit Application
--------------------------------------------------------------------------------
-- Grant usage on the Streamlit app itself
GRANT USAGE ON STREAMLIT FT_DB.FT_SCH.FirnExchange TO ROLE FIRNEXCHANGE_USER;
GRANT USAGE ON STREAMLIT FT_DB.FT_SCH.FirnExchange TO ROLE FIRNEXCHANGE_VIEWER;

-- Grant usage on the application database and schema
GRANT USAGE ON DATABASE FT_DB TO ROLE FIRNEXCHANGE_USER;
GRANT USAGE ON SCHEMA FT_DB.FT_SCH TO ROLE FIRNEXCHANGE_USER;

GRANT USAGE ON DATABASE FT_DB TO ROLE FIRNEXCHANGE_VIEWER;
GRANT USAGE ON SCHEMA FT_DB.FT_SCH TO ROLE FIRNEXCHANGE_VIEWER;

--------------------------------------------------------------------------------
-- STEP 3: Grant Warehouse Privileges
--------------------------------------------------------------------------------
-- Users need to use warehouses for query execution
-- Grant usage on specific warehouses (adjust warehouse names as needed)

-- For migration operations (needs OPERATE to start/stop warehouse)
GRANT USAGE ON WAREHOUSE XSMALL TO ROLE FIRNEXCHANGE_USER;
GRANT OPERATE ON WAREHOUSE XSMALL TO ROLE FIRNEXCHANGE_USER;

-- Add other warehouses as needed
-- GRANT USAGE ON WAREHOUSE MEDIUM TO ROLE FIRNEXCHANGE_USER;
-- GRANT OPERATE ON WAREHOUSE MEDIUM TO ROLE FIRNEXCHANGE_USER;

-- Viewers only need USAGE (can query but not start/stop)
GRANT USAGE ON WAREHOUSE XSMALL TO ROLE FIRNEXCHANGE_VIEWER;

--------------------------------------------------------------------------------
-- STEP 4: Grant Compute Pool Privileges (for Streamlit Runtime)
--------------------------------------------------------------------------------
-- Grant usage on compute pool used by the Streamlit app
GRANT USAGE ON COMPUTE POOL COMPUTE_POOL_CPU_X64_XS_1VCPU_6GB TO ROLE FIRNEXCHANGE_USER;
GRANT USAGE ON COMPUTE POOL COMPUTE_POOL_CPU_X64_XS_1VCPU_6GB TO ROLE FIRNEXCHANGE_VIEWER;

-- If using different compute pools, grant those as well
-- GRANT USAGE ON COMPUTE POOL COMPUTE_POOL_CPU_X64_S_3VCPU_13GB TO ROLE FIRNEXCHANGE_USER;

--------------------------------------------------------------------------------
-- STEP 5: Grant Privileges on DATA Databases/Schemas
--------------------------------------------------------------------------------
-- ⚠️ IMPORTANT: Customize this section for your environment
-- Grant privileges on the databases and schemas containing:
--   - Source tables for export
--   - Target Iceberg tables for import
--   - Log/tracking tables (will be created by app)

-- Example: Grant privileges on a data database
-- Replace 'YOUR_DATA_DB' and 'YOUR_DATA_SCHEMA' with actual names

-- Option A: Grant on entire database (broader access)
/*
GRANT USAGE ON DATABASE YOUR_DATA_DB TO ROLE FIRNEXCHANGE_USER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE YOUR_DATA_DB TO ROLE FIRNEXCHANGE_USER;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE YOUR_DATA_DB TO ROLE FIRNEXCHANGE_USER;
*/

-- Option B: Grant on specific schemas (recommended - more secure)
/*
GRANT USAGE ON DATABASE YOUR_DATA_DB TO ROLE FIRNEXCHANGE_USER;
GRANT USAGE ON SCHEMA YOUR_DATA_DB.YOUR_DATA_SCHEMA TO ROLE FIRNEXCHANGE_USER;
*/

-- Grant SELECT on source tables (for export operations)
/*
GRANT SELECT ON ALL TABLES IN SCHEMA YOUR_DATA_DB.YOUR_DATA_SCHEMA TO ROLE FIRNEXCHANGE_USER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA YOUR_DATA_DB.YOUR_DATA_SCHEMA TO ROLE FIRNEXCHANGE_USER;
*/

-- Grant INSERT on target Iceberg tables (for import operations)
/*
GRANT INSERT ON ALL TABLES IN SCHEMA YOUR_DATA_DB.YOUR_DATA_SCHEMA TO ROLE FIRNEXCHANGE_USER;
GRANT INSERT ON FUTURE TABLES IN SCHEMA YOUR_DATA_DB.YOUR_DATA_SCHEMA TO ROLE FIRNEXCHANGE_USER;
*/

-- Grant CREATE TABLE privilege (for creating tracking/log tables)
/*
GRANT CREATE TABLE ON SCHEMA YOUR_DATA_DB.YOUR_DATA_SCHEMA TO ROLE FIRNEXCHANGE_USER;
*/

-- Grant ALL on tracking tables (log tables created by FirnExchange)
-- Pattern: {SOURCE_TABLE_NAME}_EXPORT_FELOG and {TARGET_TABLE_NAME}_IMPORT_FELOG
/*
GRANT ALL ON ALL TABLES IN SCHEMA YOUR_DATA_DB.YOUR_DATA_SCHEMA TO ROLE FIRNEXCHANGE_USER;
*/

--------------------------------------------------------------------------------
-- STEP 6: Grant Privileges on External Stages
--------------------------------------------------------------------------------
-- ⚠️ IMPORTANT: Grant access to external stages used for data migration
-- Users need READ and WRITE access to external stages

-- Replace 'YOUR_STAGE_NAME' with actual stage names
/*
GRANT USAGE ON STAGE YOUR_DATA_DB.YOUR_DATA_SCHEMA.YOUR_STAGE_NAME TO ROLE FIRNEXCHANGE_USER;
GRANT READ ON STAGE YOUR_DATA_DB.YOUR_DATA_SCHEMA.YOUR_STAGE_NAME TO ROLE FIRNEXCHANGE_USER;
GRANT WRITE ON STAGE YOUR_DATA_DB.YOUR_DATA_SCHEMA.YOUR_STAGE_NAME TO ROLE FIRNEXCHANGE_USER;
*/

-- For Azure Blob Storage stages
/*
GRANT USAGE ON STAGE YOUR_DATA_DB.YOUR_DATA_SCHEMA.AZURE_BLOB_STAGE TO ROLE FIRNEXCHANGE_USER;
GRANT READ ON STAGE YOUR_DATA_DB.YOUR_DATA_SCHEMA.AZURE_BLOB_STAGE TO ROLE FIRNEXCHANGE_USER;
GRANT WRITE ON STAGE YOUR_DATA_DB.YOUR_DATA_SCHEMA.AZURE_BLOB_STAGE TO ROLE FIRNEXCHANGE_USER;
*/

-- For AWS S3 stages
/*
GRANT USAGE ON STAGE YOUR_DATA_DB.YOUR_DATA_SCHEMA.AWS_S3_STAGE TO ROLE FIRNEXCHANGE_USER;
GRANT READ ON STAGE YOUR_DATA_DB.YOUR_DATA_SCHEMA.AWS_S3_STAGE TO ROLE FIRNEXCHANGE_USER;
GRANT WRITE ON STAGE YOUR_DATA_DB.YOUR_DATA_SCHEMA.AWS_S3_STAGE TO ROLE FIRNEXCHANGE_USER;
*/

-- For GCS stages
/*
GRANT USAGE ON STAGE YOUR_DATA_DB.YOUR_DATA_SCHEMA.GCS_STAGE TO ROLE FIRNEXCHANGE_USER;
GRANT READ ON STAGE YOUR_DATA_DB.YOUR_DATA_SCHEMA.GCS_STAGE TO ROLE FIRNEXCHANGE_USER;
GRANT WRITE ON STAGE YOUR_DATA_DB.YOUR_DATA_SCHEMA.GCS_STAGE TO ROLE FIRNEXCHANGE_USER;
*/

--------------------------------------------------------------------------------
-- STEP 7: Grant Information Schema Access (for metadata queries)
--------------------------------------------------------------------------------
-- FirnExchange queries INFORMATION_SCHEMA for table/column metadata
-- This is implicitly granted with USAGE on database/schema, but explicit grants
-- can be added if using fine-grained access control

-- No explicit grants needed - USAGE on database/schema provides this

--------------------------------------------------------------------------------
-- STEP 8: Grant Execute Immediate Privilege
--------------------------------------------------------------------------------
-- Required for dynamic SQL execution in the app
-- This is implicitly available to roles with appropriate table privileges

--------------------------------------------------------------------------------
-- STEP 9: Setup Role Hierarchy (Optional but Recommended)
--------------------------------------------------------------------------------
-- Grant FIRNEXCHANGE_USER role to a parent role for easier management

-- Option A: Grant to SYSADMIN for operational management
-- GRANT ROLE FIRNEXCHANGE_USER TO ROLE SYSADMIN;

-- Option B: Create a custom data engineering role hierarchy
-- GRANT ROLE FIRNEXCHANGE_USER TO ROLE DATA_ENGINEER;

-- Grant viewer role to user role (users can switch between roles)
GRANT ROLE FIRNEXCHANGE_VIEWER TO ROLE FIRNEXCHANGE_USER;

--------------------------------------------------------------------------------
-- STEP 10: Grant Role to Users
--------------------------------------------------------------------------------
-- Grant the FIRNEXCHANGE_USER role to specific users

-- Example: Grant to individual users
-- GRANT ROLE FIRNEXCHANGE_USER TO USER john.doe@company.com;
-- GRANT ROLE FIRNEXCHANGE_USER TO USER jane.smith@company.com;

-- Example: Grant viewer role for monitoring only
-- GRANT ROLE FIRNEXCHANGE_VIEWER TO USER monitor.user@company.com;

-- Set as default role for user (optional)
-- ALTER USER john.doe@company.com SET DEFAULT_ROLE = FIRNEXCHANGE_USER;

--------------------------------------------------------------------------------
-- STEP 11: Verification
--------------------------------------------------------------------------------
-- Verify role was created
SHOW ROLES LIKE 'FIRNEXCHANGE_%';

-- View all grants to the role
SHOW GRANTS TO ROLE FIRNEXCHANGE_USER;

-- View users with this role
SHOW GRANTS OF ROLE FIRNEXCHANGE_USER;

-- Test role privileges (switch to role and verify)
-- USE ROLE FIRNEXCHANGE_USER;
-- SHOW DATABASES;
-- SHOW STAGES IN SCHEMA YOUR_DATA_DB.YOUR_DATA_SCHEMA;
-- SHOW WAREHOUSES;

/*==============================================================================
  COMPLETE EXAMPLE: Setting up for a specific environment
  
  Uncomment and customize the following example for a real deployment:
==============================================================================*/

/*
-- Example: Setup for SALES_DB.PROD_SCHEMA with AZURE_BLOB_STAGE

USE ROLE SECURITYADMIN;

-- 1. Grant database and schema access
GRANT USAGE ON DATABASE SALES_DB TO ROLE FIRNEXCHANGE_USER;
GRANT USAGE ON SCHEMA SALES_DB.PROD_SCHEMA TO ROLE FIRNEXCHANGE_USER;

-- 2. Grant table privileges
GRANT SELECT ON ALL TABLES IN SCHEMA SALES_DB.PROD_SCHEMA TO ROLE FIRNEXCHANGE_USER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA SALES_DB.PROD_SCHEMA TO ROLE FIRNEXCHANGE_USER;
GRANT INSERT ON ALL TABLES IN SCHEMA SALES_DB.PROD_SCHEMA TO ROLE FIRNEXCHANGE_USER;
GRANT INSERT ON FUTURE TABLES IN SCHEMA SALES_DB.PROD_SCHEMA TO ROLE FIRNEXCHANGE_USER;
GRANT CREATE TABLE ON SCHEMA SALES_DB.PROD_SCHEMA TO ROLE FIRNEXCHANGE_USER;

-- 3. Grant stage access
GRANT USAGE ON STAGE SALES_DB.PROD_SCHEMA.AZURE_BLOB_STAGE TO ROLE FIRNEXCHANGE_USER;
GRANT READ ON STAGE SALES_DB.PROD_SCHEMA.AZURE_BLOB_STAGE TO ROLE FIRNEXCHANGE_USER;
GRANT WRITE ON STAGE SALES_DB.PROD_SCHEMA.AZURE_BLOB_STAGE TO ROLE FIRNEXCHANGE_USER;

-- 4. Grant warehouse access
GRANT USAGE ON WAREHOUSE MEDIUM TO ROLE FIRNEXCHANGE_USER;
GRANT OPERATE ON WAREHOUSE MEDIUM TO ROLE FIRNEXCHANGE_USER;

-- 5. Grant to users
GRANT ROLE FIRNEXCHANGE_USER TO USER john.doe@company.com;
GRANT ROLE FIRNEXCHANGE_USER TO USER jane.smith@company.com;

-- 6. Verify
SHOW GRANTS TO ROLE FIRNEXCHANGE_USER;
*/

/*==============================================================================
  SECURITY BEST PRACTICES
==============================================================================*/

-- 1. Principle of Least Privilege
--    - Only grant access to databases/schemas that users need
--    - Don't use GRANT ALL unless absolutely necessary
--    - Use specific schema-level grants instead of database-level

-- 2. Separate Roles for Different Environments
--    - Create separate roles for DEV, QA, and PROD
--    - Example: FIRNEXCHANGE_USER_PROD, FIRNEXCHANGE_USER_DEV

-- 3. Regular Access Review
--    - Periodically review: SHOW GRANTS TO ROLE FIRNEXCHANGE_USER;
--    - Remove access when users no longer need it

-- 4. Audit Trail
--    - Monitor usage: SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
--                     WHERE ROLE_NAME = 'FIRNEXCHANGE_USER';

-- 5. Stage Security
--    - Ensure external stages have proper cloud storage permissions
--    - Use storage integrations instead of direct credentials when possible
--    - Limit stage paths using URL restrictions

-- 6. Tracking Table Security
--    - Log tables contain metadata about migrations
--    - Consider granting only SELECT to viewer roles on log tables
--    - Example: GRANT SELECT ON TABLE source_table_EXPORT_FELOG TO ROLE FIRNEXCHANGE_VIEWER;

/*==============================================================================
  TROUBLESHOOTING
==============================================================================*/

-- Problem: User can't see the Streamlit app
-- Solution: Ensure USAGE granted on STREAMLIT, DATABASE, and SCHEMA
--   GRANT USAGE ON STREAMLIT FT_DB.FT_SCH.FirnExchange TO ROLE FIRNEXCHANGE_USER;
--   GRANT USAGE ON DATABASE FT_DB TO ROLE FIRNEXCHANGE_USER;
--   GRANT USAGE ON SCHEMA FT_DB.FT_SCH TO ROLE FIRNEXCHANGE_USER;

-- Problem: User can't start warehouse
-- Solution: Grant OPERATE privilege on warehouse
--   GRANT OPERATE ON WAREHOUSE XSMALL TO ROLE FIRNEXCHANGE_USER;

-- Problem: User can't see external stages
-- Solution: Grant USAGE on stage
--   GRANT USAGE ON STAGE YOUR_STAGE TO ROLE FIRNEXCHANGE_USER;

-- Problem: COPY INTO fails with permission error
-- Solution: Grant READ/WRITE on stage
--   GRANT READ ON STAGE YOUR_STAGE TO ROLE FIRNEXCHANGE_USER;
--   GRANT WRITE ON STAGE YOUR_STAGE TO ROLE FIRNEXCHANGE_USER;

-- Problem: Can't create tracking tables
-- Solution: Grant CREATE TABLE on schema
--   GRANT CREATE TABLE ON SCHEMA YOUR_SCHEMA TO ROLE FIRNEXCHANGE_USER;

-- Problem: Can't update tracking table status
-- Solution: Grant ALL privileges on tracking tables (they are created by user)
--   GRANT ALL ON TABLE source_table_EXPORT_FELOG TO ROLE FIRNEXCHANGE_USER;

/*==============================================================================
  Deployment Complete!
  
  Next Steps:
    1. Customize STEP 5 and STEP 6 for your environment
    2. Grant role to users in STEP 10
    3. Verify access using the verification queries in STEP 11
    4. Have users test the application
    5. Monitor usage and adjust privileges as needed
    
  Notes:
    - This script provides minimal privileges needed for FirnExchange
    - Always test in a non-production environment first
    - Document any custom grants for your organization
    - Review and audit role privileges regularly
==============================================================================*/
