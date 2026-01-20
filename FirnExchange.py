"""
FirnExchange - High-Performance Data Migration Tool for Snowflake
Version: 1.0
Copyright (c) 2024-2026 Snowflake Inc.

DISCLAIMER: This software is provided by Snowflake Inc. on an "AS IS" basis without 
warranties or conditions of any kind, either express or implied. By using this software, 
you acknowledge and agree that you are solely responsible for determining the appropriateness 
of using this tool and assume all risks associated with its use, including but not limited 
to data integrity, performance impacts, and operational outcomes. Snowflake Inc. shall not 
be liable for any direct, indirect, incidental, special, or consequential damages arising 
from the use of this software. You accept full responsibility for all benefits and risks 
associated with deploying and operating FirnExchange. Use at your own risk.

For complete license terms, see LICENSE.txt
"""

import streamlit as st
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
import pandas as pd
import datetime
from datetime import datetime as dt
import threading
from queue import Queue
import time
from typing import List, Dict, Any
import traceback

# Global tracking dictionary for import (persists across Streamlit reruns)
@st.cache_resource
def get_import_tracking():
    return {
        'file_status': {},
        'results': [],
        'error_message': None,
        'lock': threading.Lock()
    }

# Global control for stop execution (thread-safe, persists across reruns)
@st.cache_resource
def get_exchange_control():
    return {
        'stop_requested': False,
        'lock': threading.Lock()
    }

# Page configuration
st.set_page_config(
    page_title="FirnTransfer",
    page_icon="❄️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Snowflake theme styling
st.markdown("""
<style>
    .stApp {
        background-color: #f8f9fa;
    }
    .stButton>button {
        background-color: #29B5E8;
        color: white;
        font-weight: bold;
        border-radius: 5px;
        border: none;
        padding: 0.5rem 1rem;
    }
    .stButton>button:hover {
        background-color: #1a8fc1;
    }
    .stSelectbox, .stMultiselect {
        color: #0E1117;
    }
    h1, h2, h3 {
        color: #29B5E8;
    }
    .sidebar .sidebar-content {
        background-color: #e8f4f8;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'session' not in st.session_state:
    st.session_state.session = None
if 'connected' not in st.session_state:
    st.session_state.connected = False
if 'auto_connected' not in st.session_state:
    st.session_state.auto_connected = False
if 'connection_params' not in st.session_state:
    st.session_state.connection_params = {}
if 'running_environment' not in st.session_state:
    st.session_state.running_environment = None
if 'databases' not in st.session_state:
    st.session_state.databases = []
if 'schemas' not in st.session_state:
    st.session_state.schemas = []
if 'tables' not in st.session_state:
    st.session_state.tables = []
if 'columns' not in st.session_state:
    st.session_state.columns = []
if 'stages' not in st.session_state:
    st.session_state.stages = []
if 'migration_running' not in st.session_state:
    st.session_state.migration_running = False
if 'migration_stats' not in st.session_state:
    st.session_state.migration_stats = {'pending': 0, 'in_progress': 0, 'completed': 0, 'failed': 0}
if 'tracking_table_name' not in st.session_state:
    st.session_state.tracking_table_name = None
if 'selected_database' not in st.session_state:
    st.session_state.selected_database = None
if 'selected_schema' not in st.session_state:
    st.session_state.selected_schema = None
if 'selected_table' not in st.session_state:
    st.session_state.selected_table = None

def validate_sis_environment():
    """
    Validate that the application is running in Streamlit in Snowflake (SiS).
    Returns True if in SiS, False otherwise.
    """
    try:
        from snowflake.snowpark.context import get_active_session
        get_active_session()
        return True
    except:
        return False

def get_snowpark_session():
    """
    Get Snowpark session from Streamlit in Snowflake environment.
    This application ONLY runs in SiS environment.
    Returns: session object
    """
    try:
        from snowflake.snowpark.context import get_active_session
        session = get_active_session()
        st.session_state.running_environment = "Streamlit in Snowflake"
        print("[Environment] Running in Streamlit in Snowflake")
        return session
    except Exception as e:
        st.session_state.running_environment = "Invalid"
        print(f"[Environment] Error: Cannot get active session - {str(e)}")
        return None

def get_databases(session):
    """Get list of databases"""
    try:
        result = session.sql("SHOW DATABASES").collect()
        return [row['name'] for row in result]
    except Exception as e:
        st.error(f"Error fetching databases: {str(e)}")
        return []

def get_schemas(session, database):
    """Get list of schemas in a database"""
    try:
        result = session.sql(f'SHOW SCHEMAS IN DATABASE "{database}"').collect()
        return [row['name'] for row in result]
    except Exception as e:
        st.error(f"Error fetching schemas: {str(e)}")
        return []

def get_tables(session, database, schema):
    """Get list of tables in a schema"""
    try:
        result = session.sql(f'SHOW TABLES IN SCHEMA "{database}"."{schema}"').collect()
        return [row['name'] for row in result]
    except Exception as e:
        st.error(f"Error fetching tables: {str(e)}")
        return []

def get_columns(session, database, schema, table):
    """Get list of columns in a table"""
    try:
        result = session.sql(f'DESCRIBE TABLE "{database}"."{schema}"."{table}"').collect()
        return [(row['name'], row['type']) for row in result]
    except Exception as e:
        st.error(f"Error fetching columns: {str(e)}")
        return []

def get_external_stages(session):
    """Get list of external stages"""
    try:
        result = session.sql("""SHOW STAGES IN ACCOUNT ->> SELECT "database_name" ||'.' || "schema_name" || '.' || "name" "name" from $1 where "type" = 'EXTERNAL'""").collect()
        return [row['name'] for row in result]
    except Exception as e:
        st.error(f"Error fetching stages: {str(e)}")
        return []

def check_log_table_exists(session, database, schema, table_name, operation_type='export'):
    """Check if log table exists for the given table
    
    Args:
        operation_type: 'export' or 'import'
    """
    log_table_name = f"{table_name}_{operation_type.upper()}_FELOG"
    try:
        check_sql = f"""
        SELECT COUNT(*) as cnt 
        FROM "{database}".INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{schema}' 
        AND TABLE_NAME = '{log_table_name}'
        """
        result = session.sql(check_sql).collect()
        exists = result[0]['CNT'] > 0
        return exists, log_table_name
    except Exception as e:
        print(f"[Log Table Check] Error: {str(e)}")
        return False, log_table_name

def create_import_log_table(session, database, schema, table_name):
    """Create log table for import operations"""
    log_table_name = f"{table_name}_IMPORT_FELOG"
    log_table = f'"{database}"."{schema}"."{log_table_name}"'
    
    try:
        # Check if table exists
        check_sql = f"""
        SELECT COUNT(*) as cnt 
        FROM "{database}".INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{schema}' 
        AND TABLE_NAME = '{log_table_name}'
        """
        result = session.sql(check_sql).collect()
        table_exists = result[0]['CNT'] > 0
        
        if table_exists:
            print(f"[Import Log Table] Reusing existing log table: {log_table}")
            return log_table
        else:
            print(f"[Import Log Table] Creating new log table: {log_table}")
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {log_table} (
                FILE_PATH STRING,
                FILE_STATUS STRING DEFAULT 'PENDING',
                IMPORT_START_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                IMPORT_END_AT TIMESTAMP,
                ERROR_MESSAGE STRING,
                ROWS_LOADED BIGINT,
                ROWS_PARSED BIGINT
            )
            """
            session.sql(create_sql).collect()
            return log_table
    except Exception as e:
        st.error(f"Error creating import log table: {str(e)}")
        return None

def create_tracking_table(session, database, schema, table_name, partition_columns, column_types_dict):
    """Create tracking table for migration - reuses existing if present"""
    # Create consistent tracking table name based on source table (for export)
    tracking_table_name = f"{table_name}_EXPORT_FELOG"
    tracking_table = f'"{database}"."{schema}"."{tracking_table_name}"'
    
    # Build column definitions for partition keys
    partition_col_defs = []
    for col in partition_columns:
        col_type = column_types_dict.get(col, 'STRING')
        partition_col_defs.append(f'"{col}" {col_type}')
    
    partition_cols_sql = ', '.join(partition_col_defs)
    
    # Check if table exists
    try:
        check_sql = f"""
        SELECT COUNT(*) as cnt 
        FROM "{database}".INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{schema}' 
        AND TABLE_NAME = '{tracking_table_name}'
        """
        result = session.sql(check_sql).collect()
        table_exists = result[0]['CNT'] > 0
        
        if table_exists:
            print(f"[Tracking Table] Reusing existing tracking table: {tracking_table}")
            st.info(f"Reusing existing tracking table: {tracking_table_name}")
            return tracking_table
        else:
            print(f"[Tracking Table] Creating new tracking table: {tracking_table}")
            create_sql = f"""
            -- CREATE HYBRID TABLE IF NOT EXISTS {tracking_table} (
            CREATE TABLE IF NOT EXISTS {tracking_table} (
                --ID INT AUTOINCREMENT PRIMARY KEY,
                {partition_cols_sql} PRIMARY KEY,
                TOTAL_ROWS BIGINT,
                PARTITION_MIGRATED_STATUS STRING DEFAULT 'NOT_SELECTED',
                PARTITION_MIGRATED_START_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                PARTITION_MIGRATED_END_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                ERROR_MESSAGE STRING,
                RETRY_COUNT INT DEFAULT 0,
                ROWS_UNLOADED BIGINT,
                INPUT_BYTES BIGINT,
                OUTPUT_BYTES BIGINT
            )
            """
            
            session.sql(create_sql).collect()
            return tracking_table
    except Exception as e:
        st.error(f"Error creating tracking table: {str(e)}")
        return None

def populate_tracking_table(session, tracking_table, source_database, source_schema, source_table, partition_columns):
    """Populate tracking table with distinct partition values - only adds new partitions"""
    try:
        partition_cols = ', '.join([f'"{col}"' for col in partition_columns])
        
        # Check if tracking table already has data
        count_result = session.sql(f"SELECT COUNT(*) as cnt FROM {tracking_table}").collect()
        existing_count = count_result[0]['CNT']
        
        if existing_count > 0:
            print(f"[Tracking Table] Found {existing_count} existing partitions in tracking table")
            st.info(f"Found {existing_count} existing partitions in tracking table. Only new partitions will be added.")
            
            # Insert only new partitions that don't exist in tracking table
            # Build the join condition
            join_conditions = []
            for col in partition_columns:
                join_conditions.append(f't."{col}" = s."{col}"')
            join_clause = ' AND '.join(join_conditions)
            
            insert_sql = f"""
            INSERT INTO {tracking_table} ({partition_cols}, TOTAL_ROWS, PARTITION_MIGRATED_STATUS)
            SELECT s.{partition_cols}, COUNT(*) as TOTAL_ROWS, 'NOT_SELECTED' as PARTITION_MIGRATED_STATUS
            FROM "{source_database}"."{source_schema}"."{source_table}" s
            LEFT JOIN {tracking_table} t ON {join_clause}
            WHERE t."{partition_columns[0]}" IS NULL
            GROUP BY s.{partition_cols}
            """
        else:
            print(f"[Tracking Table] No existing partitions, inserting all")
            # Insert all partitions
            insert_sql = f"""
            INSERT INTO {tracking_table} ({partition_cols}, TOTAL_ROWS)
            SELECT {partition_cols}, COUNT(*) as TOTAL_ROWS
            FROM "{source_database}"."{source_schema}"."{source_table}"
            GROUP BY {partition_cols}
            """
        
        session.sql(insert_sql).collect()
        
        # Get final count of all partitions (not just PENDING)
        final_count_result = session.sql(f"SELECT COUNT(*) as cnt FROM {tracking_table}").collect()
        total_count = final_count_result[0]['CNT']
        print(f"[Tracking Table] {total_count} total partitions discovered")
        st.success(f"{total_count} partitions discovered. Select partitions below to export.")
        
        return True
    except Exception as e:
        st.error(f"Error populating tracking table: {str(e)}")
        print(f"[Tracking Table] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def load_log_table_data_as_list(session, log_table_name, database, schema):
    """Load data from export log table and return as list of simple dicts (no pandas)"""
    try:
        log_table = f'"{database}"."{schema}"."{log_table_name}"'
        
        # First, get column names using DESCRIBE TABLE
        desc_result = session.sql(f"DESCRIBE TABLE {log_table}").collect()
        col_names = []
        for row in desc_result:
            try:
                # Extract column name - try different ways
                if hasattr(row, 'name'):
                    col_names.append(str(row.name))
                elif 'name' in row:
                    col_names.append(str(row['name']))
                else:
                    # Try accessing by index 0
                    col_names.append(str(row[0]))
            except Exception as e:
                print(f"[Load Log Table] Error extracting column name: {e}")
        
        print(f"[Load Log Table] Column names: {col_names}")
        
        # Now query the data
        query = f"""
            SELECT * FROM {log_table} 
            ORDER BY PARTITION_MIGRATED_STATUS, PARTITION_MIGRATED_START_AT
        """
        
        # Collect results
        result = session.sql(query).collect()
        
        if not result:
            return []
        
        # Convert to simple list of dicts using column names
        data = []
        for i, row in enumerate(result):
            row_dict = {'_index': i}  # Add index for selection tracking
            
            # Use column names we got from DESCRIBE TABLE
            for col_idx, col_name in enumerate(col_names):
                try:
                    val = row[col_idx]
                    row_dict[col_name] = str(val) if val is not None else ''
                except Exception as e:
                    print(f"[Load Log Table] Error accessing row[{col_idx}]: {e}")
                    row_dict[col_name] = ''
            
            data.append(row_dict)
        
        print(f"[Load Log Table] Successfully loaded {len(data)} rows")
        return data
    except Exception as e:
        print(f"[Load Log Table] Error loading log table data: {str(e)}")
        import traceback
        traceback.print_exc()
        return []

def build_where_clause(partition_columns, partition_values):
    """Build WHERE clause for partition"""
    conditions = []
    for col, val in zip(partition_columns, partition_values):
        if val is None:
            conditions.append(f'"{col}" IS NULL')
        elif isinstance(val, (str, datetime.date, datetime.datetime)):
            # Quote strings, dates, and timestamps
            conditions.append(f'"{col}" = \'{val}\'')
        else:
            # Numeric types don't need quotes
            conditions.append(f'"{col}" = {val}')
    return ' AND '.join(conditions)

def migrate_partition(tracking_table, source_database, source_schema, source_table, 
                     partition_columns, partition_values, stage, stage_path, warehouse, overwrite=True, single_file=True, max_file_size=5368709120, max_retries=3):
    """Migrate a single partition using async queries (runs in thread) - SiS version with collect_nowait"""
    session = None
    retry_count = 0
    
    print(f"[Partition Thread] Starting migration for partition: {partition_values}")
    
    try:
        # Get session from Snowflake context (SiS)
        from snowflake.snowpark.context import get_active_session
        session = get_active_session()
        print(f"[Partition Thread] Session created for partition: {partition_values}")
        
        # Ensure correct warehouse is being used
        if warehouse:
            print(f"[Partition Thread] Setting warehouse {warehouse} for partition: {partition_values}")
            session.sql(f"USE WAREHOUSE {warehouse}").collect()
        
        where_clause = build_where_clause(partition_columns, partition_values)
        tracking_where_clause = where_clause  # Use same logic for both
        partition_key_str = ', '.join([str(v) for v in partition_values])
        
        success = False
        while retry_count <= max_retries:
            try:
                print(f"[Partition Thread] Attempt {retry_count + 1} of {max_retries + 1} for partition: {partition_values}")
                
                # Update status to IN_PROGRESS (async)
                update_job = session.sql(f"""
                UPDATE {tracking_table}
                SET PARTITION_MIGRATED_STATUS = 'IN_PROGRESS',
                    PARTITION_MIGRATED_START_AT = CURRENT_TIMESTAMP(),
                    RETRY_COUNT = {retry_count}
                WHERE {tracking_where_clause}
                """).collect_nowait()
                
                # Wait for update to complete
                update_job.result()
                
                # Execute COPY INTO (async)
                copy_sql = f"""
                COPY INTO @{stage}/{stage_path}/{partition_key_str}/
                FROM (
                    SELECT * FROM "{source_database}"."{source_schema}"."{source_table}"
                    WHERE {where_clause}
                )
                FILE_FORMAT = (TYPE = PARQUET)
                MAX_FILE_SIZE = {max_file_size}
                OVERWRITE = {str(overwrite).upper()}
                SINGLE = {str(single_file).upper()}
                HEADER = TRUE
                DETAILED_OUTPUT = FALSE
                ;
                """
                
                print(f"[Partition Thread] Submitting async COPY INTO for partition: {partition_values}")
                copy_job = session.sql(copy_sql).collect_nowait()
                
                # Poll until complete (non-blocking with timeout)
                max_wait_seconds = 3600  # 1 hour timeout
                poll_interval = 2  # Check every 2 seconds
                elapsed = 0
                
                while not copy_job.is_done() and elapsed < max_wait_seconds:
                    time.sleep(poll_interval)
                    elapsed += poll_interval
                
                if not copy_job.is_done():
                    raise Exception(f"COPY INTO timed out after {max_wait_seconds} seconds")
                
                # Get result
                copy_result = copy_job.result()
                print(f"[Partition Thread] COPY INTO completed for partition: {partition_values}")
                
                # Extract COPY INTO results
                rows_unloaded = 0
                input_bytes = 0
                output_bytes = 0
                
                if copy_result and len(copy_result) > 0:
                    result_row = copy_result[0]
                    try:
                        result_dict = result_row.asDict()
                        rows_unloaded = result_dict.get('rows_unloaded', 0)
                        input_bytes = result_dict.get('input_bytes', 0)
                        output_bytes = result_dict.get('output_bytes', 0)
                    except (KeyError, AttributeError) as e:
                        print(f"[Partition Thread] Warning: Could not extract stats from COPY result: {e}")
                    
                    print(f"[Partition Thread] Stats - Rows unloaded: {rows_unloaded}, Input: {input_bytes} bytes, Output: {output_bytes} bytes")
                
                # Update status to COMPLETED (async)
                complete_job = session.sql(f"""
                UPDATE {tracking_table}
                SET PARTITION_MIGRATED_STATUS = 'COMPLETED',
                    PARTITION_MIGRATED_END_AT = CURRENT_TIMESTAMP(),
                    ERROR_MESSAGE = NULL,
                    ROWS_UNLOADED = {rows_unloaded},
                    INPUT_BYTES = {input_bytes},
                    OUTPUT_BYTES = {output_bytes}
                WHERE {tracking_where_clause}
                """).collect_nowait()
                
                # Wait for completion
                complete_job.result()
                
                print(f"[Partition Thread] Successfully completed partition: {partition_values}")
                success = True
                break  # Success, exit retry loop
                
            except Exception as e:
                retry_count += 1
                error_msg = str(e).replace("'", "''")[:500]  # Escape quotes and limit length
                print(f"[Partition Thread] Error on attempt {retry_count} for partition {partition_values}: {error_msg}")
                
                if retry_count > max_retries:
                    # Final failure
                    print(f"[Partition Thread] All {max_retries + 1} attempts failed for partition: {partition_values}")
                    fail_job = session.sql(f"""
                    UPDATE {tracking_table}
                    SET PARTITION_MIGRATED_STATUS = 'FAILED',
                        PARTITION_MIGRATED_END_AT = CURRENT_TIMESTAMP(),
                        ERROR_MESSAGE = '{error_msg}',
                        RETRY_COUNT = {retry_count - 1}
                    WHERE {tracking_where_clause}
                    """).collect_nowait()
                    fail_job.result()
                    break  # Exit loop after marking as failed
                else:
                    # Retry with exponential backoff
                    sleep_time = 2 ** retry_count
                    print(f"[Partition Thread] Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                    
    except Exception as e:
        print(f"[Partition Thread] Outer exception for partition {partition_values}: {str(e)}")
    finally:
        # Don't close session in SiS mode - Streamlit manages session lifecycle
        pass

def import_file_thread(file_path, target_database, target_schema, target_table, stage_name, load_mode, warehouse, control_dict, log_table=None):
    """Thread function to import a single file using async queries - SiS version"""
    session = None
    try:
        # Get session from Snowflake context (SiS)
        from snowflake.snowpark.context import get_active_session
        session = get_active_session()
        
        # Set warehouse explicitly
        if warehouse:
            session.sql(f"USE WAREHOUSE {warehouse}").collect()
        
        # Check stop signal before starting
        with control_dict['lock']:
            if control_dict['stop_requested']:
                return
        
        # Update log table - set status to IN_PROGRESS (async)
        if log_table:
            try:
                safe_file_path = file_path.replace("'", "''")
                update_job = session.sql(f"""
                UPDATE {log_table}
                SET FILE_STATUS = 'IN_PROGRESS',
                    IMPORT_START_AT = CURRENT_TIMESTAMP()
                WHERE FILE_PATH = '{safe_file_path}'
                """).collect_nowait()
                update_job.result()  # Wait for completion
            except Exception as e:
                print(f"[Import Thread] Warning: Could not update log table to IN_PROGRESS: {str(e)}")
        
        # Build fully qualified table name
        fully_qualified_table = f"{target_database}.{target_schema}.{target_table}"
        
        # Build the COPY INTO command with load_mode and fixed parameters
        copy_command = f"""
        COPY INTO {fully_qualified_table}
        FROM @{stage_name}/{file_path}
        FILE_FORMAT = (TYPE = PARQUET USE_VECTORIZED_SCANNER = TRUE)
        MATCH_BY_COLUMN_NAME = CASE_SENSITIVE
        ON_ERROR = ABORT_STATEMENT
        LOAD_MODE = {load_mode}
        """
        
        # Submit async COPY INTO
        print(f"[Import Thread] Submitting async COPY INTO for file: {file_path}")
        copy_job = session.sql(copy_command).collect_nowait()
        
        # Poll until complete (with timeout)
        max_wait_seconds = 3600  # 1 hour timeout
        poll_interval = 2
        elapsed = 0
        
        while not copy_job.is_done() and elapsed < max_wait_seconds:
            time.sleep(poll_interval)
            elapsed += poll_interval
        
        if not copy_job.is_done():
            raise Exception(f"COPY INTO timed out after {max_wait_seconds} seconds")
        
        # Get result
        result = copy_job.result()
        
        # Extract rows loaded and parsed from result
        rows_loaded = 0
        rows_parsed = 0
        if result:
            try:
                rows_loaded = result[0]['rows_loaded'] if 'rows_loaded' in result[0].asDict() else 0
                rows_parsed = result[0]['rows_parsed'] if 'rows_parsed' in result[0].asDict() else 0
            except:
                pass
        
        # Check stop signal after operation
        with control_dict['lock']:
            if control_dict['stop_requested']:
                return
        
        print(f"[Import Thread] Successfully imported file: {file_path} (rows_loaded={rows_loaded}, rows_parsed={rows_parsed})")
        
        # Update log table - set status to SUCCESS (async)
        if log_table:
            try:
                safe_file_path = file_path.replace("'", "''")
                success_job = session.sql(f"""
                UPDATE {log_table}
                SET FILE_STATUS = 'SUCCESS',
                    IMPORT_END_AT = CURRENT_TIMESTAMP(),
                    ROWS_LOADED = {rows_loaded},
                    ROWS_PARSED = {rows_parsed}
                WHERE FILE_PATH = '{safe_file_path}'
                """).collect_nowait()
                success_job.result()  # Wait for completion
            except Exception as e:
                print(f"[Import Thread] Warning: Could not update log table to SUCCESS: {str(e)}")
        
        # Update file status in session state (thread-safe)
        with control_dict['lock']:
            if 'completed_files' not in control_dict:
                control_dict['completed_files'] = []
            control_dict['completed_files'].append(file_path)
        
    except Exception as e:
        error_msg = str(e).replace("'", "''")
        print(f"[Import Thread] Error importing {file_path}: {error_msg}")
        
        # Update log table - set status to FAILED (async)
        if log_table and session:
            try:
                safe_file_path = file_path.replace("'", "''")
                safe_error_msg = error_msg[:500]  # Limit error message length
                fail_job = session.sql(f"""
                UPDATE {log_table}
                SET FILE_STATUS = 'FAILED',
                    IMPORT_END_AT = CURRENT_TIMESTAMP(),
                    ERROR_MESSAGE = '{safe_error_msg}'
                WHERE FILE_PATH = '{safe_file_path}'
                """).collect_nowait()
                fail_job.result()  # Wait for completion
            except Exception as log_e:
                print(f"[Import Thread] Warning: Could not update log table to FAILED: {str(log_e)}")
        
        # Update failed files
        with control_dict['lock']:
            if 'failed_files' not in control_dict:
                control_dict['failed_files'] = []
            control_dict['failed_files'].append({'file': file_path, 'error': error_msg})
    
    finally:
        # Don't close session in SiS mode - Streamlit manages session lifecycle
        pass

def format_elapsed_time(elapsed_seconds):
    """Format elapsed time as days, hours, minutes, seconds"""
    days = int(elapsed_seconds // 86400)
    hours = int((elapsed_seconds % 86400) // 3600)
    minutes = int((elapsed_seconds % 3600) // 60)
    seconds = int(elapsed_seconds % 60)
    
    parts = []
    if days > 0:
        parts.append(f"{days} day{'s' if days != 1 else ''}")
    if hours > 0:
        parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
    if minutes > 0:
        parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
    parts.append(f"{seconds} second{'s' if seconds != 1 else ''}")
    
    return ", ".join(parts)

def get_migration_stats(session, tracking_table):
    """Get migration statistics - excludes NOT_SELECTED partitions"""
    try:
        result = session.sql(f"""
        SELECT 
            PARTITION_MIGRATED_STATUS,
            COUNT(*) as COUNT
        FROM {tracking_table}
        GROUP BY PARTITION_MIGRATED_STATUS
        """).collect()
        
        stats = {'PENDING': 0, 'IN_PROGRESS': 0, 'COMPLETED': 0, 'FAILED': 0}
        for row in result:
            status = row['PARTITION_MIGRATED_STATUS']
            # Only count partitions that are part of the migration (exclude NOT_SELECTED)
            if status in stats:
                stats[status] = row['COUNT']
        
        return stats
    except Exception as e:
        return {'PENDING': 0, 'IN_PROGRESS': 0, 'COMPLETED': 0, 'FAILED': 0}

def get_tracking_data(session, tracking_table):
    """Get tracking table data for display - returns list of dicts using simple types"""
    try:
        # Simple query without complex type conversions
        query = f"""
            SELECT * 
            FROM {tracking_table} 
            ORDER BY PARTITION_MIGRATED_START_AT DESC
        """
        result = session.sql(query).collect()
        
        if not result:
            return []
        
        # Get column names from first row as pure Python strings
        first_row_dict = result[0].asDict()
        column_names = [str(k) for k in first_row_dict.keys()]
        
        # Build data as list of dicts with pure Python types
        data = []
        for row in result:
            row_dict = {}
            for i, col in enumerate(column_names):
                try:
                    # Access by index to avoid complex key types
                    val = row[i]
                    # Convert to simple Python string
                    row_dict[col] = str(val) if val is not None else None
                except Exception as e:
                    print(f"[Tracking Data] Error accessing row[{i}]: {str(e)}")
                    row_dict[col] = None
            data.append(row_dict)
        
        return data
    except Exception as e:
        st.error(f"Error fetching tracking data: {str(e)}")
        print(f"[Tracking Data] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return []

# Main UI
# Environment badge in top right corner
if st.session_state.running_environment == "Streamlit in Snowflake":
    st.markdown('<div style="position: fixed; top: 10px; right: 60px; z-index: 999; background: #f0f2f6; padding: 5px 10px; border-radius: 5px; font-weight: bold;">🏔️ SiS</div>', unsafe_allow_html=True)
elif st.session_state.running_environment == "Local":
    st.markdown('<div style="position: fixed; top: 10px; right: 60px; z-index: 999; background: #f0f2f6; padding: 5px 10px; border-radius: 5px; font-weight: bold;">💻 Local</div>', unsafe_allow_html=True)

# Centered title
st.markdown("""
<div style="text-align: center; margin-bottom: 2rem;">
    <h1 style="color: #29B5E8; margin-bottom: 0;">❄️ FirnExchange</h1>
    <h3 style="color: #666; margin-top: 0.5rem;">High-Performance Data Exchange Iceberg Migration</h3>
</div>
""", unsafe_allow_html=True)

# Create tabs
# Only FirnExchange tab
tab1 = st.container()

# Validate environment before proceeding
if not validate_sis_environment():
    st.error("⚠️ **Invalid Environment**")
    st.markdown("""
    ### This Application Runs ONLY in Snowflake
    
    **FirnExchange** is designed exclusively for **Streamlit in Snowflake (SiS)** environment.
    
    **To run this application:**
    1. Deploy it to your Snowflake account using the provided deployment script
    2. Access it through Snowflake's Streamlit interface
    3. Navigate to: **Data Products > Streamlit > FirnExchange**
    
    **Deployment Instructions:**
    - Review the `firnexchange_sis.sql` deployment script
    - Execute the script in your Snowflake account
    - The application will be available in your Snowflake account
    
    **Why SiS Only?**
    - ✅ Enhanced security with native Snowflake authentication
    - ✅ No external dependencies or configuration files
    - ✅ Seamless integration with Snowflake resources
    - ✅ Centralized deployment and access control
    - ✅ Better performance with native Snowflake connectivity
    
    ---
    **Note:** This application does not support local execution and has no dependency on `config.toml` files.
    """)
    st.stop()

# Sidebar - Connection Management (SiS only)
with st.sidebar:
    st.header("Connection")
    
    # Auto-connect to Snowflake session
    if not st.session_state.connected:
        session = get_snowpark_session()
        
        if session:
            st.session_state.session = session
            st.session_state.connected = True
            st.session_state.auto_connected = True
            st.session_state.running_environment = "Streamlit in Snowflake"
            
            # Get connection info from active session
            try:
                current_db = session.get_current_database()
                current_schema = session.get_current_schema()
                current_warehouse = session.get_current_warehouse()
                current_role = session.get_current_role()
                current_user = session.get_current_user()
                
                # Get account name using SQL
                current_account = 'N/A'
                try:
                    account_result = session.sql("SELECT current_account() as account").collect()
                    if account_result:
                        current_account = account_result[0]['ACCOUNT']
                except:
                    pass
                
                st.session_state.connection_params = {
                    'account': current_account,
                    'database': current_db,
                    'schema': current_schema,
                    'warehouse': current_warehouse,
                    'role': current_role,
                    'user': current_user
                }
            except:
                st.session_state.connection_params = {}
            
            # Load metadata
            st.session_state.stages = get_external_stages(session)
            st.session_state.databases = get_databases(session)
            
            st.success("🏔️ Connected to Snowflake")
            st.rerun()
        else:
            st.error("❌ Failed to connect to Snowflake")
            st.stop()
    
    # Show environment indicator
    st.info("🏔️ **Running in Snowflake**")
    
    # Show connection details
    if st.session_state.connected:
        st.divider()
        st.subheader("Connection Details")
        connection_params = st.session_state.connection_params
        
        # Get account name from SQL if not in params
        account_name = connection_params.get('account', 'N/A')
        if account_name == 'N/A' and st.session_state.session:
            try:
                account_result = st.session_state.session.sql("SELECT current_account() as account").collect()
                if account_result:
                    account_name = account_result[0]['ACCOUNT']
            except:
                account_name = 'N/A'
        
        st.write(f"**Account:** {account_name}")
        st.write(f"**User:** {connection_params.get('user', 'N/A')}")
        st.write(f"**Role:** {connection_params.get('role', 'N/A')}")
        st.write(f"**Warehouse:** {connection_params.get('warehouse', 'N/A')}")
        
    # Environment info
    if st.session_state.connected:
        st.divider()
        with st.expander("ℹ️ Environment Info", expanded=False):
            st.markdown("""
            **Streamlit in Snowflake**
            - ✅ Native Snowflake authentication
            - ✅ Secure execution context
            - ✅ No external configuration required
            - ✅ Centralized access control
            - ✅ All features supported
            """)

# Tab 1: FirnExchange - Combined Export and Import
with tab1:
    st.subheader("Unified Data Export & Import")
    
    if st.session_state.connected:
        session = st.session_state.session
        
        # Initialize session state for FirnExchange
        if 'exchange_operation' not in st.session_state:
            st.session_state.exchange_operation = "FDN Table Data Export"
        if 'exchange_selected_warehouse' not in st.session_state:
            st.session_state.exchange_selected_warehouse = None
        if 'exchange_selected_stage' not in st.session_state:
            st.session_state.exchange_selected_stage = None
        if 'exchange_migration_running' not in st.session_state:
            st.session_state.exchange_migration_running = False
        if 'exchange_tracking_table_name' not in st.session_state:
            st.session_state.exchange_tracking_table_name = None
        if 'exchange_import_has_results' not in st.session_state:
            st.session_state.exchange_import_has_results = False
        if 'exchange_calculated_max_workers' not in st.session_state:
            st.session_state.exchange_calculated_max_workers = 366
        if 'exchange_default_workers' not in st.session_state:
            st.session_state.exchange_default_workers = 366
        if 'exchange_selected_partitions' not in st.session_state:
            st.session_state.exchange_selected_partitions = []
        if 'exchange_partitions_list' not in st.session_state:
            st.session_state.exchange_partitions_list = []
        if 'exchange_log_table_loaded' not in st.session_state:
            st.session_state.exchange_log_table_loaded = False
        
        # Warehouse and Worker Configuration Section
        with st.expander("🏭 Warehouse & Worker Configuration", expanded=True):
            col1, col2 = st.columns(2)
            
            with col1:
                # Load warehouses if not already loaded
                if 'warehouses' not in st.session_state or not st.session_state.warehouses:
                    st.session_state.warehouses = []
                    st.session_state.warehouse_details = {}
                    try:
                        result = session.sql("SHOW WAREHOUSES").collect()
                        for row in result:
                            warehouse_name = row['name']
                            st.session_state.warehouses.append(warehouse_name)
                            row_dict = row.asDict()
                            st.session_state.warehouse_details[warehouse_name] = {
                                'type': row_dict.get('type', 'N/A'),
                                'size': row_dict.get('size', 'N/A'),
                                'min_cluster_count': row_dict.get('min_cluster_count', 'N/A'),
                                'max_cluster_count': row_dict.get('max_cluster_count', 'N/A'),
                                'enable_query_acceleration': row_dict.get('enable_query_acceleration', 'N/A'),
                                'query_acceleration_max_scale_factor': row_dict.get('query_acceleration_max_scale_factor', 'N/A'),
                                'scaling_policy': row_dict.get('scaling_policy', 'N/A'),
                                'warehouse_credit_limit': row_dict.get('warehouse_credit_limit', 'N/A')
                            }
                    except Exception as e:
                        st.error(f"Error loading warehouses: {str(e)}")
                
                if st.session_state.warehouses:
                    exchange_warehouse = st.selectbox(
                        "Warehouse",
                        st.session_state.warehouses,
                        index=0,
                        help="Select the warehouse to use for the operation",
                        key="exchange_warehouse_select"
                    )
                    st.session_state.exchange_selected_warehouse = exchange_warehouse
                    
                    # Display warehouse properties and calculate worker limits
                    if exchange_warehouse and exchange_warehouse in st.session_state.warehouse_details:
                        st.caption("**Warehouse Properties:**")
                        wh_details = st.session_state.warehouse_details[exchange_warehouse]
                        st.caption(f"🔹 **Type:** {wh_details['type']}")
                        st.caption(f"🔹 **Size:** {wh_details['size']}")
                        st.caption(f"🔹 **Cluster Count:** {wh_details['min_cluster_count']} - {wh_details['max_cluster_count']}")
                        st.caption(f"🔹 **Scaling Policy:** {wh_details['scaling_policy']}")
                        
                        # Get max_cluster_count
                        max_cluster_count = wh_details['max_cluster_count']
                        
                        # Fetch and display warehouse parameters
                        max_concurrency_level = 8  # Default value if not found
                        try:
                            params_result = session.sql(f"SHOW PARAMETERS FOR WAREHOUSE {exchange_warehouse}").collect()
                            if params_result:
                                st.caption("**Warehouse Parameters:**")
                                for row in params_result:
                                    row_dict = row.asDict()
                                    param_key = row_dict.get('key', '')
                                    param_value = row_dict.get('value', '')
                                    if param_key and param_value:
                                        st.caption(f"🔹 **{param_key}:** {param_value}")
                                        # Capture MAX_CONCURRENCY_LEVEL
                                        if param_key == 'MAX_CONCURRENCY_LEVEL':
                                            try:
                                                max_concurrency_level = int(param_value)
                                            except (ValueError, TypeError):
                                                max_concurrency_level = 8
                        except Exception as e:
                            print(f"[FirnExchange] Error fetching warehouse parameters: {str(e)}")
                        
                        # Calculate dynamic max workers
                        calculated_max_workers = max_cluster_count * max_concurrency_level
                        
                        # Set default value to the maximum calculated value
                        default_workers = calculated_max_workers
                        
                        # Store in session state for use in slider
                        st.session_state.exchange_calculated_max_workers = calculated_max_workers
                        st.session_state.exchange_default_workers = default_workers
                        
                        # print(f"[FirnExchange] Warehouse: {exchange_warehouse}, Max Cluster: {max_cluster_count}, Max Concurrency: {max_concurrency_level}")
                        # print(f"[FirnExchange] Calculated Max Workers: {calculated_max_workers}, Default: {default_workers}")
                else:
                    st.warning("No warehouses available")
                    exchange_warehouse = None
                    # Set defaults if no warehouse selected
                    st.session_state.exchange_calculated_max_workers = 366
                    st.session_state.exchange_default_workers = 366
            
            with col2:
                # Get dynamic values from session state
                slider_max_value = st.session_state.get('exchange_calculated_max_workers', 366)
                slider_default_value = st.session_state.get('exchange_default_workers', 366)
                
                # Ensure default doesn't exceed max
                slider_default_value = min(slider_default_value, slider_max_value)
                
                exchange_max_workers = st.slider(
                    "Max Concurrent Workers",
                    min_value=1,
                    max_value=slider_max_value,
                    value=slider_default_value,
                    step=1,
                    help=f"Maximum number of concurrent threads (calculated: {slider_max_value} = max_cluster_count × MAX_CONCURRENCY_LEVEL)",
                    key="exchange_max_workers_slider"
                )
                current_operation = st.session_state.get('exchange_operation', 'FDN Table Data Export')
                st.caption(f"Using up to **{exchange_max_workers}** parallel threads for data {current_operation.split()[-1].lower()}")
                
                # Show calculation info if warehouse is selected
                if exchange_warehouse and exchange_warehouse in st.session_state.warehouse_details:
                    wh_details = st.session_state.warehouse_details[exchange_warehouse]
                    max_cluster = wh_details['max_cluster_count']
                    st.caption(f"ℹ️ Max value calculated: {slider_max_value} (Max Clusters: {max_cluster} × Max Concurrency: {slider_max_value // max_cluster})")
        
        st.divider()
        
        # Stage Configuration Section
        with st.expander("📦 Stage Configuration", expanded=True):
            col1, col2 = st.columns(2)
            
            with col1:
                # Load stages if not already loaded
                if 'stages' not in st.session_state or not st.session_state.stages:
                    st.session_state.stages = get_external_stages(session)
                
                if st.session_state.stages:
                    exchange_stage = st.selectbox(
                        "External Stage",
                        st.session_state.stages,
                        key="exchange_stage_select"
                    )
                    st.session_state.exchange_selected_stage = exchange_stage
                    
                    # Display stage properties
                    if exchange_stage:
                        try:
                            # Parse stage name to get database and schema
                            stage_parts = exchange_stage.split('.')
                            if len(stage_parts) == 3:
                                stage_db = stage_parts[0]
                                stage_sch = stage_parts[1]
                                stage_name = stage_parts[2]
                                show_stage_cmd = f"SHOW STAGES LIKE '{stage_name}' IN {stage_db}.{stage_sch}"
                            else:
                                show_stage_cmd = f"SHOW STAGES LIKE '{exchange_stage}'"
                            
                            stage_result = session.sql(show_stage_cmd).collect()
                            if stage_result:
                                stage_info = stage_result[0].asDict()
                                
                                # Display stage properties
                                st.caption("**Stage Properties:**")
                                
                                # Extract and display the requested properties
                                url = stage_info.get('url', 'N/A')
                                region = stage_info.get('region', 'N/A')
                                stage_type = stage_info.get('type', 'N/A')
                                storage_integration = stage_info.get('storage_integration', 'N/A')
                                cloud = stage_info.get('cloud', 'N/A')
                                
                                # Create a compact display
                                prop_col1, prop_col2 = st.columns(2)
                                with prop_col1:
                                    st.caption(f"🔹 **Type:** {stage_type}")
                                    st.caption(f"🔹 **Cloud:** {cloud}")
                                    st.caption(f"🔹 **Region:** {region}")
                                with prop_col2:
                                    st.caption(f"🔹 **URL:** {url}")
                                    st.caption(f"🔹 **Storage Integration:** {storage_integration}")
                        except Exception as e:
                            print(f"[Stage Properties] Error fetching stage properties: {str(e)}")
                else:
                    st.warning("No external stages available")
                    exchange_stage = None
            
            with col2:
                exchange_stage_path = st.text_input(
                    "Relative Path in Stage",
                    value="",
                    placeholder="path/to/data",
                    key="exchange_stage_path_input"
                )
            
            # File listing for stage (useful for both export and import)
            if exchange_stage:
                if st.button("List Files in Stage", key="exchange_list_files"):
                    try:
                        if exchange_stage_path:
                            list_command = f"LIST @{exchange_stage}/{exchange_stage_path}"
                        else:
                            list_command = f"LIST @{exchange_stage}"
                        
                        print(f"[FirnExchange] Executing: {list_command}")
                        # Collect and convert to avoid pandas conversion issues with complex types
                        result = session.sql(list_command).collect()
                        
                        if result:
                            # Convert to list of dicts with simple types using JSON serialization
                            import json
                            import pandas as pd
                            data = []
                            for row in result:
                                # Get row as dict first
                                row_dict_raw = row.asDict()
                                row_dict = {}
                                for field_name, value in row_dict_raw.items():
                                    # Ensure field name is a simple string
                                    field_name_str = str(field_name)
                                    
                                    # Convert value to JSON-safe type
                                    try:
                                        json.dumps(value)
                                        row_dict[field_name_str] = value
                                    except (TypeError, ValueError):
                                        row_dict[field_name_str] = str(value)
                                data.append(row_dict)
                            
                            # Use JSON round-trip to ensure all data is JSON-safe
                            try:
                                json_str = json.dumps(data)
                                clean_data = json.loads(json_str)
                                files_df = pd.DataFrame(clean_data)
                            except Exception as e:
                                print(f"[List Files] JSON conversion failed: {str(e)}, falling back to string conversion")
                                string_data = []
                                for row_dict in data:
                                    string_dict = {str(k): str(v) if v is not None else None for k, v in row_dict.items()}
                                    string_data.append(string_dict)
                                files_df = pd.DataFrame(string_data)
                            st.session_state.exchange_files_df = files_df
                            st.session_state.exchange_selected_files = []
                            st.success(f"Found {len(files_df)} file(s)")
                        else:
                            st.warning("No files found in the specified path")
                            st.session_state.exchange_files_df = None
                    except Exception as e:
                        st.error(f"Error listing files: {str(e)}")
                        st.session_state.exchange_files_df = None
                
                # Display files table with checkboxes
                if 'exchange_files_df' in st.session_state and st.session_state.exchange_files_df is not None:
                    st.subheader("Files in Stage")
                    
                    # Filter option
                    col_filter1, col_filter2 = st.columns([2, 4])
                    with col_filter1:
                        file_filter_option = st.radio(
                            "Show:",
                            ["Files and Folders", "Files Only"],
                            index=0,
                            horizontal=True,
                            key="exchange_file_filter"
                        )
                    
                    files_df = st.session_state.exchange_files_df.copy()
                    
                    # Apply filter if "Files Only" is selected
                    # Keep track of original indices before filtering
                    if file_filter_option == "Files Only":
                        # Filter to show only files with size > 0, but keep original index
                        files_df = files_df[files_df['size'] > 0]
                        # Store original indices before reset
                        original_indices = files_df.index.tolist()
                        # Reset index for display but keep the mapping
                        files_df = files_df.reset_index(drop=True)
                        st.caption(f"📄 Showing files only (size > 0): {len(files_df)} file(s)")
                    else:
                        original_indices = list(range(len(files_df)))
                        st.caption(f"📁 Showing files and folders: {len(files_df)} item(s)")
                    
                    if 'exchange_selected_files' not in st.session_state:
                        st.session_state.exchange_selected_files = []
                    
                    # Store original dataframe for operations
                    original_files_df = st.session_state.exchange_files_df
                    
                    # Use the original_indices we saved above for operations
                    filtered_indices = original_indices
                    
                    # Select All / Select None / Delete buttons
                    col_btn1, col_btn2, col_btn3, col_btn4, col_btn5 = st.columns([1, 1, 1, 1, 2])
                    with col_btn1:
                        if st.button("Select All", key="exchange_select_all_files"):
                            # Select all items in the filtered view
                            st.session_state.exchange_selected_files = filtered_indices
                            st.rerun()
                    with col_btn2:
                        if st.button("Select None", key="exchange_select_none_files"):
                            st.session_state.exchange_selected_files = []
                            st.rerun()
                    with col_btn3:
                        if st.button("Delete Selected", key="exchange_delete_selected_files", type="secondary"):
                            if st.session_state.exchange_selected_files:
                                try:
                                    deleted_count = 0
                                    failed_count = 0
                                    for idx in st.session_state.exchange_selected_files:
                                        file_name = original_files_df.loc[idx, 'name']
                                        # Extract relative path from S3 URL if needed
                                        if file_name.startswith('s3://') and exchange_stage_path:
                                            # Extract path after the stage relative path
                                            search_pattern = f'/{exchange_stage_path}/'
                                            if search_pattern in file_name:
                                                split_pos = file_name.index(search_pattern) + len(search_pattern)
                                                relative_file_path = file_name[split_pos:]
                                            else:
                                                relative_file_path = file_name.split('/')[-1]
                                        else:
                                            relative_file_path = file_name.split('/')[-1]
                                        
                                        if exchange_stage_path:
                                            rm_command = f"REMOVE @{exchange_stage}/{exchange_stage_path}/{relative_file_path}"
                                        else:
                                            rm_command = f"REMOVE @{exchange_stage}/{relative_file_path}"
                                        
                                        try:
                                            print(f"[FirnExchange] Executing: {rm_command}")
                                            session.sql(rm_command).collect()
                                            deleted_count += 1
                                        except Exception as e:
                                            print(f"[FirnExchange] Error deleting {file_name}: {str(e)}")
                                            failed_count += 1
                                    
                                    if failed_count == 0:
                                        st.success(f"✅ Deleted {deleted_count} file(s) successfully")
                                    else:
                                        st.warning(f"⚠️ Deleted {deleted_count} file(s), failed to delete {failed_count} file(s)")
                                    
                                    # Clear selection and refresh file list
                                    st.session_state.exchange_selected_files = []
                                    st.session_state.exchange_files_df = None
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error deleting files: {str(e)}")
                            else:
                                st.warning("No files selected")
                    with col_btn4:
                        # Initialize confirmation state
                        if 'exchange_delete_all_confirm' not in st.session_state:
                            st.session_state.exchange_delete_all_confirm = False
                        
                        if not st.session_state.exchange_delete_all_confirm:
                            if st.button("Delete All", key="exchange_delete_all_files", type="secondary"):
                                st.session_state.exchange_delete_all_confirm = True
                                st.rerun()
                        else:
                            if st.button("⚠️ Confirm Delete All", key="exchange_confirm_delete_all", type="primary"):
                                try:
                                    if exchange_stage_path:
                                        rm_command = f"REMOVE @{exchange_stage}/{exchange_stage_path}"
                                    else:
                                        rm_command = f"REMOVE @{exchange_stage}"
                                    
                                    print(f"[FirnExchange] Executing: {rm_command}")
                                    result = session.sql(rm_command).collect()
                                    st.success(f"✅ Deleted all files from stage path")
                                    
                                    # Clear selection and refresh file list
                                    st.session_state.exchange_selected_files = []
                                    st.session_state.exchange_files_df = None
                                    st.session_state.exchange_delete_all_confirm = False
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error deleting all files: {str(e)}")
                                    st.session_state.exchange_delete_all_confirm = False
                    with col_btn5:
                        if st.session_state.exchange_delete_all_confirm:
                            if st.button("Cancel", key="exchange_cancel_delete_all"):
                                st.session_state.exchange_delete_all_confirm = False
                                st.rerun()
                    
                    # Create mapping from filtered to original indices for checkbox handling
                    # Map display index (0, 1, 2...) to original dataframe index
                    filtered_to_original_map = {i: orig_idx for i, orig_idx in enumerate(original_indices)}
                    # print(f"[FirnExchange] Filter: {file_filter_option}, Mapping: {filtered_to_original_map}")
                    
                    st.write(f"**Selected: {len(st.session_state.exchange_selected_files)} of {len(original_files_df)} total files**")
                    
                    # Create a dataframe with checkbox column
                    display_df = files_df.copy()
                    display_df.insert(0, 'Select', False)
                    
                    # Mark selected files in the filtered view
                    for display_idx in range(len(display_df)):
                        orig_idx = filtered_to_original_map[display_idx]
                        if orig_idx in st.session_state.exchange_selected_files:
                            display_df.at[display_idx, 'Select'] = True
                    
                    # Display the interactive dataframe with sortable columns
                    edited_df = st.data_editor(
                        display_df,
                        hide_index=True,
                        use_container_width=True,
                        height=400,
                        disabled=[col for col in display_df.columns if col != 'Select'],
                        column_config={
                            "Select": st.column_config.CheckboxColumn("Select", help="Select files", default=False),
                            "name": st.column_config.TextColumn("File Name", width="large"),
                            "size": st.column_config.NumberColumn("Size (bytes)", format="%d"),
                            "md5": st.column_config.TextColumn("MD5", width="medium"),
                            "last_modified": st.column_config.TextColumn("Last Modified", width="medium"),
                        },
                        key="exchange_file_editor"
                    )
                    
                    # Update selection mapping back to original indices
                    new_selected = []
                    for display_idx in edited_df[edited_df['Select']].index.tolist():
                        orig_idx = filtered_to_original_map[display_idx]
                        new_selected.append(orig_idx)
                    st.session_state.exchange_selected_files = new_selected
        
        st.divider()
        
        # Operation Configuration Section
        with st.expander("⚙️ Operation Configuration", expanded=True):
            operation = st.selectbox(
                "Select Operation",
                ["FDN Table Data Export", "Iceberg Table Data Import"],
                key="exchange_operation_select"
            )
            st.session_state.exchange_operation = operation
            st.info(f"**Selected Operation:** {operation}")
        
            # Export-specific configuration
            if operation == "FDN Table Data Export":
                st.divider()
                st.subheader("Export Options")
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    # Initialize session state for export options
                    if 'exchange_overwrite' not in st.session_state:
                        st.session_state.exchange_overwrite = True
                    
                    exchange_overwrite = st.radio(
                        "OVERWRITE",
                        [True, False],
                        index=0 if st.session_state.exchange_overwrite else 1,
                        key="exchange_overwrite_radio",
                        help="Overwrite existing files in the stage"
                    )
                    st.session_state.exchange_overwrite = exchange_overwrite
                
                with col2:
                    # Initialize session state for single file option
                    if 'exchange_single_file' not in st.session_state:
                        st.session_state.exchange_single_file = True
                    
                    exchange_single_file = st.radio(
                        "SINGLE FILE",
                        [True, False],
                        index=0 if st.session_state.exchange_single_file else 1,
                        key="exchange_single_file_radio",
                        help="Export to a single file per partition"
                    )
                    st.session_state.exchange_single_file = exchange_single_file
                
                with col3:
                    # Initialize session state for max file size
                    if 'exchange_max_file_size' not in st.session_state:
                        st.session_state.exchange_max_file_size = 5368709120
                    
                    exchange_max_file_size = st.number_input(
                        "MAX FILE SIZE (bytes)",
                        min_value=1,
                        max_value=5368709120,
                        value=st.session_state.exchange_max_file_size,
                        step=1000000,
                        key="exchange_max_file_size_input",
                        help="Maximum file size in bytes (max: 5368709120 = 5GB)"
                    )
                    st.session_state.exchange_max_file_size = exchange_max_file_size
                
                st.caption(f"**Export Settings:** OVERWRITE={exchange_overwrite}, SINGLE={exchange_single_file}, MAX_FILE_SIZE={exchange_max_file_size:,}")
        
        st.divider()
        
        # Operation-Specific Configuration
        if operation == "FDN Table Data Export":
            st.header("Source Table Configuration")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                exchange_src_database = st.selectbox(
                    "Source Database",
                    st.session_state.databases if st.session_state.databases else ["Select database"],
                    key="exchange_src_db"
                )
            
            with col2:
                if exchange_src_database and exchange_src_database != "Select database":
                    exchange_src_schemas = get_schemas(session, exchange_src_database)
                    exchange_src_schema = st.selectbox(
                        "Source Schema",
                        exchange_src_schemas if exchange_src_schemas else ["Select schema"],
                        key="exchange_src_schema"
                    )
                else:
                    exchange_src_schema = st.selectbox("Source Schema", ["Select schema"], key="exchange_src_schema_disabled")
                    exchange_src_schemas = []
            
            with col3:
                if exchange_src_schema and exchange_src_schema != "Select schema":
                    exchange_src_tables = get_tables(session, exchange_src_database, exchange_src_schema)
                    
                    # Preserve table selection across reruns
                    if 'exchange_src_table_selection' not in st.session_state:
                        st.session_state.exchange_src_table_selection = None
                    
                    # Find index of previously selected table
                    table_index = 0
                    if st.session_state.exchange_src_table_selection in exchange_src_tables:
                        table_index = exchange_src_tables.index(st.session_state.exchange_src_table_selection)
                    
                    exchange_src_table = st.selectbox(
                        "Source Table",
                        exchange_src_tables if exchange_src_tables else ["Select table"],
                        index=table_index,
                        key="exchange_src_table"
                    )
                    
                    # Store selection in session state
                    st.session_state.exchange_src_table_selection = exchange_src_table
                else:
                    exchange_src_table = st.selectbox("Source Table", ["Select table"], key="exchange_src_table_disabled")
                    exchange_src_tables = []
                    st.session_state.exchange_src_table_selection = None
            
            # Partition Key Selection for Export
            if exchange_src_table and exchange_src_table != "Select table":
                exchange_columns = get_columns(session, exchange_src_database, exchange_src_schema, exchange_src_table)
                
                if exchange_columns:
                    column_names = [col[0] for col in exchange_columns]
                    column_types_dict = {col[0]: col[1] for col in exchange_columns}
                    
                    st.subheader("Partition Key Column")
                    default_partition = column_names[0] if column_names else None
                    exchange_partition_column = st.selectbox(
                        "Select partition key column (single column only)",
                        column_names,
                        index=0 if column_names else 0,
                        key="exchange_partition_cols"
                    )
                    # Convert to list for compatibility with existing code
                    exchange_partition_columns = [exchange_partition_column] if exchange_partition_column else []
                    
                    # Check if log table exists for export
                    log_exists, log_table_name = check_log_table_exists(
                        session, exchange_src_database, exchange_src_schema, exchange_src_table, 'export'
                    )
                    
                    st.divider()
                    st.subheader("Log Table Configuration")
                    
                    if log_exists:
                        st.info(f"📋 Log table exists: `{log_table_name}`")
                        
                        # Initialize session state for truncate option
                        if 'exchange_truncate_log' not in st.session_state:
                            st.session_state.exchange_truncate_log = False
                        
                        # Action buttons for existing log table
                        col_log_action1, col_log_action2 = st.columns(2)
                        
                        with col_log_action1:
                            exchange_truncate_log = st.radio(
                                "Log Table Action",
                                ["Reuse existing log table", "Truncate log table before export"],
                                index=0 if not st.session_state.exchange_truncate_log else 1,
                                key="exchange_truncate_log_radio"
                            )
                            st.session_state.exchange_truncate_log = (exchange_truncate_log == "Truncate log table before export")
                        
                        with col_log_action2:
                            st.write("")  # Spacing
                            st.write("")  # Spacing
                            
                            # Drop log table button
                            if st.button("🗑️ Drop Log Table", key="export_drop_log_table", type="secondary"):
                                try:
                                    # Use fully qualified path for drop table
                                    fully_qualified_log_table = f'"{exchange_src_database}"."{exchange_src_schema}"."{log_table_name}"'
                                    with st.spinner(f"Dropping log table `{log_table_name}`..."):
                                        session.sql(f"DROP TABLE IF EXISTS {fully_qualified_log_table}").collect()
                                        st.success(f"✅ Log table `{log_table_name}` dropped successfully")
                                        # Clear all partition-related session state
                                        st.session_state.exchange_log_table_loaded = False
                                        st.session_state.exchange_selected_partitions = []
                                        st.session_state.exchange_partitions_list = []
                                        st.session_state.exchange_tracking_table_name = None
                                        st.rerun()
                                except Exception as e:
                                    st.error(f"Error dropping log table: {str(e)}")
                            
                            # Re-analyze partitions button
                            if st.button("🔄 Re-Analyze Partitions", key="export_reanalyze_partitions", type="secondary"):
                                if not exchange_partition_columns:
                                    st.error("Please select a partition key column")
                                else:
                                    with st.spinner("Re-analyzing partitions..."):
                                        try:
                                            # Drop existing log table
                                            session.sql(f"DROP TABLE IF EXISTS {log_table_name}").collect()
                                            
                                            # Create tracking table
                                            tracking_table = create_tracking_table(
                                                session, exchange_src_database, exchange_src_schema, exchange_src_table,
                                                exchange_partition_columns, column_types_dict
                                            )
                                            
                                            if tracking_table:
                                                # Populate tracking table
                                                if populate_tracking_table(
                                                    session, tracking_table, exchange_src_database,
                                                    exchange_src_schema, exchange_src_table, exchange_partition_columns
                                                ):
                                                    st.success(f"✅ Partition re-analysis complete! Log table recreated: {log_table_name}")
                                                    st.session_state.exchange_tracking_table_name = tracking_table
                                                    st.session_state.exchange_log_table_loaded = False
                                                    st.session_state.exchange_selected_partitions = []
                                                    st.session_state.exchange_partitions_list = []
                                                    st.rerun()
                                                else:
                                                    st.error("Failed to populate tracking table")
                                            else:
                                                st.error("Failed to create tracking table")
                                        except Exception as e:
                                            st.error(f"Error during re-analysis: {str(e)}")
                        
                        # Load and display partition data with selection
                        st.divider()
                        
                        # Load log table data as list of dicts (avoiding pandas)
                        # Skip reloading if export is running and we already have the data
                        if st.session_state.exchange_migration_running and st.session_state.exchange_partitions_list:
                            partitions_list = st.session_state.exchange_partitions_list
                        else:
                            partitions_list = load_log_table_data_as_list(session, log_table_name, exchange_src_database, exchange_src_schema)
                        
                        if partitions_list:
                            st.session_state.exchange_partitions_list = partitions_list
                            st.session_state.exchange_log_table_loaded = True
                            
                            # Get column names from first partition (excluding internal fields)
                            col_names = [k for k in partitions_list[0].keys() if not k.startswith('_')]
                            
                            # Initialize checkbox render version (used to force checkbox recreation)
                            if 'exchange_checkbox_version' not in st.session_state:
                                st.session_state.exchange_checkbox_version = 0
                            
                            # Wrap partition selection table in collapsible expander (collapsed by default)
                            with st.expander("🔍 Partition Selection Table", expanded=False):
                                # Filter buttons
                                col_filter1, col_filter2, col_filter3 = st.columns(3)
                                
                                with col_filter1:
                                    if st.button("✅ Select All", key="export_select_all"):
                                        st.session_state.exchange_selected_partitions = list(range(len(partitions_list)))
                                        # Increment version to force checkbox recreation
                                        st.session_state.exchange_checkbox_version += 1
                                        st.rerun()
                                
                                with col_filter2:
                                    if st.button("🔄 Select Non-Completed", key="export_select_non_completed"):
                                        # Select rows where status is not 'COMPLETED'
                                        non_completed_indices = []
                                        for i, partition in enumerate(partitions_list):
                                            if partition.get('PARTITION_MIGRATED_STATUS', '') != 'COMPLETED':
                                                non_completed_indices.append(i)
                                        st.session_state.exchange_selected_partitions = non_completed_indices
                                        # Increment version to force checkbox recreation
                                        st.session_state.exchange_checkbox_version += 1
                                        st.rerun()
                                
                                with col_filter3:
                                    if st.button("❌ Unselect All", key="export_unselect_all"):
                                        st.session_state.exchange_selected_partitions = []
                                        # Increment version to force checkbox recreation
                                        st.session_state.exchange_checkbox_version += 1
                                        st.rerun()
                                
                                # Display partitions with interactive checkboxes in table-like format
                                # Track checkbox states and sync with session state
                                new_selected_partitions = []
                                
                                # Create table-like display with inline checkboxes
                                # Limit columns to display (first 6 columns to keep it manageable)
                                display_cols = col_names[:6] if len(col_names) > 6 else col_names
                                
                                # Add custom CSS for table-like appearance
                                st.markdown("""
                                <style>
                                .partition-row {
                                    border-bottom: 1px solid #e0e0e0;
                                    padding: 5px 0;
                                }
                                .partition-header {
                                    background: #f0f2f6;
                                    font-weight: bold;
                                    padding: 10px 5px;
                                    border-bottom: 2px solid #29B5E8;
                                    margin-bottom: 5px;
                                }
                                </style>
                                """, unsafe_allow_html=True)
                                
                                # Create header row
                                header_html = '<div class="partition-header" style="display: grid; grid-template-columns: 50px '
                                header_html += ' '.join(['1fr'] * len(display_cols))
                                header_html += '; gap: 5px; align-items: center;">'
                                header_html += '<div style="text-align: center;">Select</div>'
                                for col in display_cols:
                                    # Shorten column names if too long
                                    display_name = col if len(col) <= 20 else col[:17] + '...'
                                    header_html += f'<div>{display_name}</div>'
                                header_html += '</div>'
                                st.markdown(header_html, unsafe_allow_html=True)
                                
                                # Container for scrollable content
                                with st.container():
                                    # Display each row with checkbox and data
                                    for i, partition in enumerate(partitions_list):
                                        is_selected = i in st.session_state.exchange_selected_partitions
                                        
                                        # Determine status for color coding
                                        status = partition.get('PARTITION_MIGRATED_STATUS', '')
                                        if status == 'COMPLETED':
                                            status_icon = '✅'
                                            bg_style = 'background: #d4edda;'
                                        elif status == 'FAILED':
                                            status_icon = '❌'
                                            bg_style = 'background: #f8d7da;'
                                        elif status == 'IN_PROGRESS':
                                            status_icon = '⏳'
                                            bg_style = 'background: #fff3cd;'
                                        elif is_selected:
                                            status_icon = '📋'
                                            bg_style = 'background: #d1ecf1;'
                                        else:
                                            status_icon = ''
                                            bg_style = ''
                                        
                                        # Create row with columns
                                        cols = st.columns([0.5] + [1] * len(display_cols))
                                        
                                        # Checkbox column
                                        with cols[0]:
                                            # Include version in key to force recreation when buttons are clicked
                                            checkbox_key = f"export_partition_checkbox_{i}_v{st.session_state.exchange_checkbox_version}"
                                            checkbox_value = st.checkbox(
                                                "",
                                                value=is_selected,
                                                key=checkbox_key,
                                                label_visibility="collapsed"
                                            )
                                            if checkbox_value:
                                                new_selected_partitions.append(i)
                                        
                                        # Data columns
                                        for idx, col in enumerate(display_cols, start=1):
                                            with cols[idx]:
                                                value = partition.get(col, '')
                                                # Format value for display
                                                if value is None:
                                                    display_value = ''
                                                elif isinstance(value, (int, float)):
                                                    if isinstance(value, float):
                                                        display_value = f'{value:,.2f}'
                                                    else:
                                                        display_value = f'{value:,}'
                                                else:
                                                    display_value = str(value)
                                                    # Truncate very long values
                                                    if len(display_value) > 30:
                                                        display_value = display_value[:27] + '...'
                                                
                                                # Display with status indicator
                                                if idx == 1 and status_icon:  # Add icon to first data column
                                                    st.markdown(f"{status_icon} {display_value}")
                                                else:
                                                    st.markdown(f"{display_value}")
                                        
                                        # Add separator line
                                        if i < len(partitions_list) - 1:
                                            st.markdown('<hr style="margin: 2px 0; border: none; border-top: 1px solid #e0e0e0;">', unsafe_allow_html=True)
                                
                                # Update session state with new selection from checkboxes
                                st.session_state.exchange_selected_partitions = new_selected_partitions
                            
                            # Show selection summary AFTER expander (after checkboxes are processed)
                            total_partitions = len(partitions_list)
                            selected_count = len(st.session_state.exchange_selected_partitions)
                            st.info(f"📊 **Selected: {selected_count} of {total_partitions} partitions** (expand above to modify selection)")
                                
                        else:
                            st.warning("⚠️ Cannot load partition data for selection (technical limitation with complex data types). The export will process all PENDING partitions in the log table when you click 'Start Export'.")
                            st.caption(f"You can manually query the log table to see partitions: `SELECT * FROM {log_table_name}`")
                            st.session_state.exchange_log_table_loaded = False
                    else:
                        st.info(f"📋 Log table will be created: `{log_table_name}`")
                        st.session_state.exchange_truncate_log = False  # New table, no need to truncate
                        st.session_state.exchange_log_table_loaded = False
                        
                        # Show "Analyze Partitions" button
                        st.divider()
                        st.subheader("Partition Analysis")
                        
                        if st.button("🔍 Analyze Partitions", key="export_analyze_partitions", type="primary"):
                            if not exchange_partition_columns:
                                st.error("Please select a partition key column")
                            else:
                                with st.spinner("Analyzing partitions..."):
                                    # Create tracking table
                                    tracking_table = create_tracking_table(
                                        session, exchange_src_database, exchange_src_schema, exchange_src_table,
                                        exchange_partition_columns, column_types_dict
                                    )
                                    
                                    if tracking_table:
                                        # Populate tracking table
                                        if populate_tracking_table(
                                            session, tracking_table, exchange_src_database,
                                            exchange_src_schema, exchange_src_table, exchange_partition_columns
                                        ):
                                            st.success(f"Partition analysis complete! Log table created: {log_table_name}")
                                            st.session_state.exchange_tracking_table_name = tracking_table
                                            st.rerun()
                                        else:
                                            st.error("Failed to populate tracking table")
                                    else:
                                        st.error("Failed to create tracking table")
            
            # Start/Stop Export Buttons
            col_btn1, col_btn2 = st.columns([1, 1])
            
            # Determine if Start Export should be disabled
            # If log table wasn't loaded successfully, allow export without partition selection
            log_table_loaded = st.session_state.get('exchange_log_table_loaded', False)
            export_disabled = (
                st.session_state.exchange_migration_running or 
                (log_table_loaded and len(st.session_state.exchange_selected_partitions) == 0)
            )
            
            with col_btn1:
                if st.button("Start Export", disabled=export_disabled, key="exchange_start_export", type="primary"):
                    print("=" * 80)
                    print("[FirnExchange Export] ★★★ START EXPORT BUTTON CLICKED ★★★")
                    print("=" * 80)
                    
                    # Guard against re-execution
                    if st.session_state.exchange_migration_running:
                        print("[FirnExchange Export] Export already running - stopping")
                        st.warning("Export is already running")
                        st.stop()
                    
                    print(f"[FirnExchange Export] Validation starting...")
                    
                    # Reset stop signal and clear import results
                    exchange_control = get_exchange_control()
                    with exchange_control['lock']:
                        exchange_control['stop_requested'] = False
                    st.session_state.exchange_import_has_results = False
                    
                    if not exchange_partition_columns:
                        print("[FirnExchange Export] ERROR: No partition column selected")
                        st.error("Please select a partition key column")
                    elif log_table_loaded and len(st.session_state.exchange_selected_partitions) == 0:
                        print("[FirnExchange Export] ERROR: No partitions selected")
                        st.error("Please select at least one partition for export")
                    elif not exchange_stage:
                        print("[FirnExchange Export] ERROR: No stage selected")
                        st.error("Please select an external stage")
                    elif not locals().get('exchange_stage_path'):
                        print("[FirnExchange Export] ERROR: No stage path entered")
                        st.error("Please enter a relative path in stage")
                    elif not exchange_warehouse:
                        print("[FirnExchange Export] ERROR: No warehouse selected")
                        st.error("Please select a warehouse")
                    else:
                        print("[FirnExchange Export] ✓ All validations passed")
                        print(f"[FirnExchange Export] Partition columns: {exchange_partition_columns}")
                        print(f"[FirnExchange Export] Stage: {exchange_stage}")
                        print(f"[FirnExchange Export] Stage path: {exchange_stage_path}")
                        print(f"[FirnExchange Export] Warehouse: {exchange_warehouse}")
                        print(f"[FirnExchange Export] Selected partitions: {len(st.session_state.exchange_selected_partitions)}")
                        
                        # Set warehouse
                        try:
                            session.sql(f"USE WAREHOUSE {exchange_warehouse}").collect()
                            st.info(f"Using warehouse: {exchange_warehouse}")
                        except Exception as e:
                            st.error(f"Error setting warehouse: {str(e)}")
                            st.stop()
                        
                        # Get tracking table name
                        log_table_name = f"{exchange_src_table}_EXPORT_FELOG"
                        tracking_table = f'"{exchange_src_database}"."{exchange_src_schema}"."{log_table_name}"'
                        st.session_state.exchange_tracking_table_name = tracking_table
                        
                        # Get selected partitions from list (not dataframe)
                        partitions_list = st.session_state.get('exchange_partitions_list', [])
                        selected_indices = st.session_state.exchange_selected_partitions
                        
                        # Extract selected partitions from the list
                        selected_partitions = [partitions_list[i] for i in selected_indices if i < len(partitions_list)]
                        
                        print(f"[FirnExchange Export] Processing {len(selected_partitions)} selected partitions")
                        
                        # Mark selected partitions as PENDING in the tracking table
                        # Note: All partitions start as 'NOT_SELECTED' after analysis
                        # Only selected partitions are changed to 'PENDING' here
                        # This ensures run_migration only processes selected partitions
                        try:
                            # Mark all selected partitions as PENDING
                            for partition_dict in selected_partitions:
                                # Build WHERE clause for this partition
                                where_conditions = []
                                for col in exchange_partition_columns:
                                    val = partition_dict.get(col, None)
                                    if val is None or val == '':
                                        where_conditions.append(f'"{col}" IS NULL')
                                    elif isinstance(val, str):
                                        where_conditions.append(f'"{col}" = \'{val}\'')
                                    else:
                                        where_conditions.append(f'"{col}" = {val}')
                                where_clause = ' AND '.join(where_conditions)
                                
                                # Update status to PENDING for selected partition
                                update_sql = f"""
                                UPDATE {tracking_table}
                                SET PARTITION_MIGRATED_STATUS = 'PENDING',
                                    RETRY_COUNT = 0,
                                    ERROR_MESSAGE = NULL
                                WHERE {where_clause}
                                """
                                session.sql(update_sql).collect()
                            
                            print(f"[FirnExchange Export] Marked {len(selected_partitions)} partitions as PENDING")
                            st.success(f"Ready to export {len(selected_partitions)} selected partitions")
                        
                        except Exception as e:
                            st.error(f"Error preparing partitions for export: {str(e)}")
                            print(f"[FirnExchange Export] Error: {str(e)}")
                            import traceback
                            traceback.print_exc()
                            st.stop()
                        
                        # Start export
                        st.session_state.exchange_migration_running = True
                        
                        # Initialize timing information
                        exchange_control = get_exchange_control()
                        with exchange_control['lock']:
                            exchange_control['start_time'] = dt.now()
                            exchange_control['end_time'] = None
                        
                        # Debug logging
                        print(f"[FirnExchange Export] Starting export with warehouse: {exchange_warehouse}")
                        print(f"[FirnExchange Export] Max workers: {exchange_max_workers}")
                        
                        # Start migration in background
                        def run_migration(warehouse, max_workers_count, control_dict, export_overwrite=True, export_single=True, export_max_size=5368709120):
                            print(f"[FirnExchange Export] run_migration function started")
                            print(f"[FirnExchange Export] Warehouse: {warehouse}, Max workers: {max_workers_count}")
                            try:
                                print(f"[FirnExchange Export] Getting session from SiS context...")
                                from snowflake.snowpark.context import get_active_session
                                bg_session = get_active_session()
                                print(f"[FirnExchange Export] Session obtained, starting migration")
                                active_threads = []
                                
                                # print(f"[FirnExchange Export] Starting migration for table: {tracking_table}")
                                # print(f"[FirnExchange Export] Max workers configured: {max_workers_count}")
                                
                                while True:
                                    # Check for stop signal (thread-safe)
                                    with control_dict['lock']:
                                        stop_requested = control_dict['stop_requested']
                                    
                                    if stop_requested:
                                        print("[FirnExchange Export] Stop requested, terminating...")
                                        # Wait for active threads with timeout
                                        for thread in active_threads:
                                            thread.join(timeout=5)
                                        # Reset stop signal
                                        with control_dict['lock']:
                                            control_dict['stop_requested'] = False
                                        break
                                    
                                    active_threads = [t for t in active_threads if t.is_alive()]
                                    available_slots = max_workers_count - len(active_threads)
                                    
                                    if available_slots > 0:
                                        pending = bg_session.sql(f"""
                                        SELECT * FROM {tracking_table}
                                        WHERE PARTITION_MIGRATED_STATUS = 'PENDING'
                                        LIMIT {available_slots}
                                        """).collect()
                                        
                                        # print(f"[FirnExchange Export] Found {len(pending)} pending partitions, active threads: {len(active_threads)}, available slots: {available_slots}")
                                        
                                        for row in pending:
                                            partition_values = [row[col] for col in exchange_partition_columns]
                                            # print(f"[FirnExchange Export] Starting thread for partition: {partition_values}")
                                            
                                            thread = threading.Thread(
                                                target=migrate_partition,
                                                args=(
                                                    tracking_table,
                                                    exchange_src_database,
                                                    exchange_src_schema,
                                                    exchange_src_table,
                                                    exchange_partition_columns,
                                                    partition_values,
                                                    exchange_stage,
                                                    exchange_stage_path,
                                                    warehouse,
                                                    export_overwrite,
                                                    export_single,
                                                    export_max_size
                                                )
                                            )
                                            thread.start()
                                            active_threads.append(thread)
                                    
                                    stats = get_migration_stats(bg_session, tracking_table)
                                    total = sum(stats.values())
                                    # print(f"[FirnExchange Export] Stats: {stats}, Total: {total}")
                                    
                                    if stats['PENDING'] == 0 and stats['IN_PROGRESS'] == 0 and total > 0:
                                        print("[FirnExchange Export] All partitions processed, exiting")
                                        break
                                    
                                    time.sleep(0.5)
                                
                                # print(f"[FirnExchange Export] Waiting for {len(active_threads)} threads to complete")
                                for thread in active_threads:
                                    thread.join()
                                
                                print("[FirnExchange Export] Migration complete")
                                # Set end time
                                with control_dict['lock']:
                                    control_dict['end_time'] = dt.now()
                                # Don't close session in SiS mode - Streamlit manages session lifecycle
                            except Exception as e:
                                print(f"[FirnExchange Export] ERROR in migration thread: {str(e)}")
                                import traceback
                                traceback.print_exc()
                                # Set end time even on error
                                with control_dict['lock']:
                                    control_dict['end_time'] = dt.now()
                        
                        # Get control dictionary and pass to thread
                        print(f"[FirnExchange Export] Starting export: {exchange_src_database}.{exchange_src_schema}.{exchange_src_table} → {exchange_stage}/{exchange_stage_path}")
                        # print(f"[FirnExchange Export] Tracking table: {tracking_table}")
                        # print(f"[FirnExchange Export] Max workers: {exchange_max_workers}")
                        # print(f"[FirnExchange Export] Warehouse: {exchange_warehouse}")
                        
                        # Get export configuration from session state
                        export_overwrite = st.session_state.get('exchange_overwrite', True)
                        export_single_file = st.session_state.get('exchange_single_file', True)
                        export_max_file_size = st.session_state.get('exchange_max_file_size', 5368709120)
                        
                        exchange_control = get_exchange_control()
                        migration_thread = threading.Thread(
                            target=run_migration, 
                            args=(exchange_warehouse, exchange_max_workers, exchange_control, export_overwrite, export_single_file, export_max_file_size)
                        )
                        migration_thread.daemon = True
                        migration_thread.start()
                        
                        # print(f"[FirnExchange Export] Migration thread started successfully")
                        
                        st.rerun()
            
            with col_btn2:
                if st.button("Stop Execution", disabled=not st.session_state.exchange_migration_running, key="exchange_stop_export", type="secondary"):
                    # Set stop signal (thread-safe)
                    exchange_control = get_exchange_control()
                    with exchange_control['lock']:
                        exchange_control['stop_requested'] = True
                    st.session_state.exchange_migration_running = False
                    st.warning("Stop signal sent. Waiting for active threads to complete...")
                    st.rerun()
        
        elif operation == "Iceberg Table Data Import":
            st.header("Target Table Configuration")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if not st.session_state.databases:
                    st.session_state.databases = get_databases(session)
                
                exchange_tgt_database = st.selectbox(
                    "Target Database",
                    st.session_state.databases if st.session_state.databases else ["Select database"],
                    key="exchange_tgt_db"
                )
            
            with col2:
                if exchange_tgt_database and exchange_tgt_database != "Select database":
                    exchange_tgt_schemas = get_schemas(session, exchange_tgt_database)
                    exchange_tgt_schema = st.selectbox(
                        "Target Schema",
                        exchange_tgt_schemas if exchange_tgt_schemas else ["Select schema"],
                        key="exchange_tgt_schema"
                    )
                else:
                    exchange_tgt_schema = st.selectbox("Target Schema", ["Select schema"], key="exchange_tgt_schema_disabled")
            
            with col3:
                if exchange_tgt_schema and exchange_tgt_schema != "Select schema":
                    try:
                        result = session.sql(f"SHOW ICEBERG TABLES IN {exchange_tgt_database}.{exchange_tgt_schema}").collect()
                        exchange_iceberg_tables = ["Select Iceberg table"] + [row['name'] for row in result]
                    except Exception as e:
                        st.error(f"Error loading Iceberg tables: {str(e)}")
                        exchange_iceberg_tables = ["Select Iceberg table"]
                    
                    exchange_tgt_table = st.selectbox(
                        "Target Iceberg Table",
                        exchange_iceberg_tables,
                        key="exchange_tgt_table"
                    )
                    
                    # Display Iceberg table properties
                    if exchange_tgt_table and exchange_tgt_table != "Select Iceberg table":
                        try:
                            show_iceberg_cmd = f"SHOW ICEBERG TABLES LIKE '{exchange_tgt_table}' IN {exchange_tgt_database}.{exchange_tgt_schema}"
                            print(f"[Iceberg Table Properties] Executing: {show_iceberg_cmd}")
                            iceberg_result = session.sql(show_iceberg_cmd).collect()
                            
                            if iceberg_result:
                                iceberg_info = iceberg_result[0].asDict()
                                
                                # Display iceberg table properties
                                st.caption("**Iceberg Table Properties:**")
                                
                                # Extract and display the requested properties (case-sensitive lowercase)
                                catalog_name = iceberg_info.get('catalog_name', 'N/A')
                                iceberg_table_type = iceberg_info.get('iceberg_table_type', 'N/A')
                                external_volume_name = iceberg_info.get('external_volume_name', 'N/A')
                                base_location = iceberg_info.get('base_location', 'N/A')
                                
                                # Create a compact display
                                ice_col1, ice_col2 = st.columns(2)
                                with ice_col1:
                                    st.caption(f"🔹 **Catalog Name:** {catalog_name}")
                                    st.caption(f"🔹 **Table Type:** {iceberg_table_type}")
                                with ice_col2:
                                    st.caption(f"🔹 **External Volume:** {external_volume_name}")
                                    st.caption(f"🔹 **Base Location:** {base_location}")
                        except Exception as e:
                            print(f"[Iceberg Table Properties] Error fetching properties: {str(e)}")
                else:
                    exchange_tgt_table = st.selectbox("Target Iceberg Table", ["Select schema first"], key="exchange_tgt_table_disabled")
            
            # Data Loading Parameters
            st.subheader("Data Loading Parameters")
            col1, col2 = st.columns(2)
            
            with col1:
                exchange_load_mode = st.selectbox(
                    "LOAD_MODE",
                    ["ADD_FILES_COPY", "ADD_FILES_REFERENCE"],
                    index=0,
                    help="Specifies how to load data into the Iceberg table",
                    key="exchange_load_mode_select"
                )
            
            with col2:
                st.info("**Fixed Parameters:**\n\n"
                       "• FILE_FORMAT: PARQUET (USE_VECTORIZED_SCANNER = TRUE)\n\n"
                       "• MATCH_BY_COLUMN_NAME: CASE_SENSITIVE\n\n"
                       "• ON_ERROR: ABORT_STATEMENT")
            
            st.caption(f"**Load Configuration:** LOAD_MODE={exchange_load_mode}, FILE_FORMAT=PARQUET (USE_VECTORIZED_SCANNER=TRUE), MATCH_BY_COLUMN_NAME=CASE_SENSITIVE, ON_ERROR=ABORT_STATEMENT")
            
            # Check if log table exists for import
            if exchange_tgt_table and exchange_tgt_table != "Select Iceberg table":
                log_exists_import, log_table_name_import = check_log_table_exists(
                    session, exchange_tgt_database, exchange_tgt_schema, exchange_tgt_table, 'import'
                )
                
                st.divider()
                st.subheader("Log Table Configuration")
                
                if log_exists_import:
                    st.info(f"📋 Log table exists: `{log_table_name_import}`")
                    
                    # Initialize session state for import truncate option
                    if 'exchange_import_truncate_log' not in st.session_state:
                        st.session_state.exchange_import_truncate_log = False
                    
                    # Action buttons for existing log table
                    col_import_log_action1, col_import_log_action2 = st.columns([2, 1])
                    
                    with col_import_log_action1:
                        exchange_import_truncate_log = st.radio(
                            "Log Table Action",
                            ["Reuse existing log table", "Truncate log table before import"],
                            index=0 if not st.session_state.exchange_import_truncate_log else 1,
                            key="exchange_import_truncate_log_radio"
                        )
                        st.session_state.exchange_import_truncate_log = (exchange_import_truncate_log == "Truncate log table before import")
                    
                    with col_import_log_action2:
                        st.write("")  # Spacing
                        st.write("")  # Spacing
                        
                        # Drop log table button
                        if st.button("🗑️ Drop Log Table", key="import_drop_log_table", type="secondary"):
                            try:
                                # Use fully qualified path for drop table
                                fully_qualified_log_table_import = f'"{exchange_tgt_database}"."{exchange_tgt_schema}"."{log_table_name_import}"'
                                with st.spinner(f"Dropping log table `{log_table_name_import}`..."):
                                    session.sql(f"DROP TABLE IF EXISTS {fully_qualified_log_table_import}").collect()
                                    st.success(f"✅ Log table `{log_table_name_import}` dropped successfully")
                                    st.rerun()
                            except Exception as e:
                                st.error(f"Error dropping log table: {str(e)}")
                else:
                    st.info(f"📋 Log table will be created: `{log_table_name_import}`")
                    st.session_state.exchange_import_truncate_log = False  # New table, no need to truncate
            
            # Start/Stop Import Buttons
            col_btn1, col_btn2 = st.columns([1, 1])
            
            with col_btn1:
                if st.button("Start Import", disabled=st.session_state.exchange_migration_running, key="exchange_start_import", type="primary"):
                    print("=" * 80)
                    print("[FirnExchange Import] ★★★ START IMPORT BUTTON CLICKED ★★★")
                    print("=" * 80)
                    
                    # Guard against re-execution
                    if st.session_state.exchange_migration_running:
                        print("[FirnExchange Import] Import already running - stopping")
                        st.warning("Import is already running")
                        st.stop()
                    
                    print(f"[FirnExchange Import] Validation starting...")
                    
                    # Reset stop signal and clear export tracking table
                    exchange_control = get_exchange_control()
                    with exchange_control['lock']:
                        exchange_control['stop_requested'] = False
                    st.session_state.exchange_tracking_table_name = None
                    
                    if not exchange_tgt_table or exchange_tgt_table == "Select Iceberg table":
                        print("[FirnExchange Import] ERROR: No target table selected")
                        st.error("Please select an Iceberg table")
                    elif not exchange_stage:
                        st.error("Please select a stage")
                    elif 'exchange_selected_files' not in st.session_state or not st.session_state.exchange_selected_files:
                        st.error("Please select at least one file to import")
                    elif not exchange_warehouse:
                        st.error("Please select a warehouse")
                    else:
                        # Set warehouse
                        try:
                            session.sql(f"USE WAREHOUSE {exchange_warehouse}").collect()
                            st.info(f"Using warehouse: {exchange_warehouse}")
                        except Exception as e:
                            st.error(f"Error setting warehouse: {str(e)}")
                            st.stop()
                        
                        # Prepare files list for import
                        if 'exchange_files_df' in st.session_state and st.session_state.exchange_files_df is not None:
                            files_df = st.session_state.exchange_files_df
                            selected_files = []
                            skipped_count = 0
                            
                            print(f"[FirnExchange Import] Selected indices: {st.session_state.exchange_selected_files}")
                            print(f"[FirnExchange Import] Total files in dataframe: {len(files_df)}")
                            
                            for idx in st.session_state.exchange_selected_files:
                                print(f"[FirnExchange Import] Processing index: {idx}")
                                file_name = files_df.loc[idx, 'name']
                                file_size = files_df.loc[idx, 'size']
                                
                                print(f"[FirnExchange Import] File at index {idx}: {file_name}, size: {file_size}")
                                
                                # Skip files with size 0 (directories and empty files)
                                if file_size == 0:
                                    print(f"[FirnExchange Import] Skipping file with size 0: {file_name}")
                                    skipped_count += 1
                                    continue
                                
                                # Extract relative path from cloud storage URL
                                # The file_name from LIST command is the full cloud URL
                                # We need to extract the path relative to the stage base location
                                relative_file_path = None
                                
                                if '://' in file_name:
                                    # Cloud storage URL (s3://, azure://, gs://, etc.)
                                    if exchange_stage_path:
                                        # Pattern to search: /{exchange_stage_path}/
                                        search_pattern = f'/{exchange_stage_path}/'
                                        print(f"[FirnExchange Import] Looking for pattern '{search_pattern}' in {file_name}")
                                        
                                        if search_pattern in file_name:
                                            # Extract everything after the stage relative path
                                            split_pos = file_name.index(search_pattern) + len(search_pattern)
                                            path_after_stage = file_name[split_pos:]
                                            # Construct full path: stage_path/remaining_path
                                            relative_file_path = f"{exchange_stage_path}/{path_after_stage}"
                                            print(f"[FirnExchange Import] Extracted relative path: {relative_file_path}")
                                        else:
                                            # Pattern not found, extract everything after the last occurrence of stage_path
                                            print(f"[FirnExchange Import] Pattern not found, trying alternative extraction")
                                            # Split by '/' and find all parts after stage_path
                                            parts = file_name.split('/')
                                            try:
                                                stage_idx = parts.index(exchange_stage_path)
                                                # Get all parts after stage_path
                                                remaining_parts = parts[stage_idx + 1:]
                                                if remaining_parts:
                                                    path_after_stage = '/'.join(remaining_parts)
                                                    relative_file_path = f"{exchange_stage_path}/{path_after_stage}"
                                                else:
                                                    # Just use the filename
                                                    relative_file_path = f"{exchange_stage_path}/{parts[-1]}"
                                                print(f"[FirnExchange Import] Alternative extraction result: {relative_file_path}")
                                            except ValueError:
                                                # stage_path not found in parts, just use filename
                                                relative_file_path = f"{exchange_stage_path}/{parts[-1]}"
                                                print(f"[FirnExchange Import] Stage path not found in URL parts, using: {relative_file_path}")
                                    else:
                                        # No stage path specified, just get the filename
                                        relative_file_path = file_name.split('/')[-1]
                                        print(f"[FirnExchange Import] No stage path, using filename: {relative_file_path}")
                                else:
                                    # Not a cloud URL, treat as local path
                                    if exchange_stage_path:
                                        relative_file_path = f"{exchange_stage_path}/{file_name.split('/')[-1]}"
                                    else:
                                        relative_file_path = file_name.split('/')[-1]
                                    print(f"[FirnExchange Import] Local path, using: {relative_file_path}")
                                
                                if relative_file_path:
                                    selected_files.append(relative_file_path)
                                    print(f"[FirnExchange Import] Final file path for COPY: {relative_file_path}")
                                else:
                                    print(f"[FirnExchange Import] ERROR: Could not extract relative path for: {file_name}")
                            
                            if len(selected_files) == 0:
                                st.error(f"No valid files to import. All {skipped_count} selected file(s) have size 0 (directories or empty files).")
                                st.stop()
                            
                            if skipped_count > 0:
                                st.info(f"Skipped {skipped_count} file(s) with size 0 (directories or empty files)")
                            
                            print(f"[FirnExchange Import] Starting import of {len(selected_files)} file(s)")
                            
                            # Create import log table
                            import_log_table = create_import_log_table(
                                session, exchange_tgt_database, exchange_tgt_schema, exchange_tgt_table
                            )
                            
                            if import_log_table:
                                # Truncate log table if user selected that option
                                if st.session_state.get('exchange_import_truncate_log', False):
                                    try:
                                        print(f"[FirnExchange Import] Truncating import log table: {import_log_table}")
                                        session.sql(f"TRUNCATE TABLE {import_log_table}").collect()
                                        print(f"[FirnExchange Import] Truncated log table")
                                    except Exception as e:
                                        st.error(f"Error truncating import log table: {str(e)}")
                                        st.stop()
                                else:
                                    print(f"[FirnExchange Import] Reusing existing import log table: {import_log_table}")
                                
                                # Insert pending files into log table (bulk insert for efficiency)
                                try:
                                    # Build bulk insert statement
                                    values_list = []
                                    for file_path in selected_files:
                                        safe_file_path = file_path.replace("'", "''")
                                        values_list.append(f"('{safe_file_path}', 'PENDING')")
                                    
                                    if values_list:
                                        # Use INSERT with ON CONFLICT handling if supported, or merge
                                        insert_sql = f"""
                                        INSERT INTO {import_log_table} (FILE_PATH, FILE_STATUS)
                                        SELECT column1, column2 FROM (VALUES {', '.join(values_list)})
                                        WHERE column1 NOT IN (SELECT FILE_PATH FROM {import_log_table})
                                        """
                                        session.sql(insert_sql).collect()
                                        print(f"[FirnExchange Import] Added files to import log table")
                                except Exception as e:
                                    st.error(f"Error populating import log table: {str(e)}")
                                    print(f"[FirnExchange Import] Error: {str(e)}")
                                    st.stop()
                            else:
                                st.error("Failed to create import log table")
                                st.stop()
                            
                            # Debug logging
                            print(f"[FirnExchange Import] Starting import with warehouse: {exchange_warehouse}")
                            print(f"[FirnExchange Import] Max workers: {exchange_max_workers}")
                            
                            # Initialize control dict for import
                            exchange_control['completed_files'] = []
                            exchange_control['failed_files'] = []
                            exchange_control['total_files'] = len(selected_files)
                            exchange_control['import_complete'] = False
                            exchange_control['stop_requested'] = False
                            exchange_control['start_time'] = dt.now()
                            exchange_control['end_time'] = None
                            
                            # Start import in background thread
                            def run_import(warehouse, files_list, max_workers_count, tgt_db, tgt_sch, tgt_tbl, stg_name, load_mode_value, control_dict, log_tbl=None):
                                print(f"[FirnExchange Import] run_import function started")
                                print(f"[FirnExchange Import] Warehouse: {warehouse}, Max workers: {max_workers_count}")
                                try:
                                    print(f"[FirnExchange Import] Getting session from SiS context...")
                                    from snowflake.snowpark.context import get_active_session
                                    bg_session = get_active_session()
                                    print(f"[FirnExchange Import] Session obtained, starting import of {len(files_list)} files")
                                    active_threads = []
                                    file_queue = files_list.copy()
                                    
                                    # print(f"[FirnExchange Import] Starting import for {len(files_list)} files into {tgt_db}.{tgt_sch}.{tgt_tbl} with LOAD_MODE={load_mode_value}")
                                    # if log_tbl:
                                    #     print(f"[FirnExchange Import] Using log table: {log_tbl}")
                                    
                                    while True:
                                        # Check stop signal
                                        with control_dict['lock']:
                                            if control_dict['stop_requested']:
                                                print("[FirnExchange Import] Stop requested, cleaning up...")
                                                # Wait for active threads to complete
                                                for t in active_threads:
                                                    t.join(timeout=5)
                                                print("[FirnExchange Import] Stopped")
                                                break
                                        
                                        # Remove completed threads
                                        active_threads = [t for t in active_threads if t.is_alive()]
                                        
                                        # Start new threads up to max_workers
                                        available_slots = max_workers_count - len(active_threads)
                                        
                                        if available_slots > 0 and len(file_queue) > 0:
                                            files_to_process = file_queue[:available_slots]
                                            file_queue = file_queue[available_slots:]
                                            
                                            for file_path in files_to_process:
                                                thread = threading.Thread(
                                                    target=import_file_thread,
                                                    args=(file_path, tgt_db, tgt_sch, tgt_tbl, 
                                                          stg_name, load_mode_value, warehouse, control_dict, log_tbl)
                                                )
                                                thread.start()
                                                active_threads.append(thread)
                                                # print(f"[FirnExchange Import] Started thread for file: {file_path} ({len(active_threads)} active)")
                                        
                                        # Check if all done
                                        if len(file_queue) == 0 and len(active_threads) == 0:
                                            print("[FirnExchange Import] All files processed")
                                            # Signal completion and set end time
                                            with control_dict['lock']:
                                                control_dict['import_complete'] = True
                                                control_dict['end_time'] = dt.now()
                                            break
                                        
                                        time.sleep(0.5)
                                    
                                    # Don't close session in SiS mode - Streamlit manages session lifecycle
                                    
                                except Exception as e:
                                    print(f"[FirnExchange Import] ERROR in import thread: {str(e)}")
                                    import traceback
                                    traceback.print_exc()
                                    # Signal completion even on error
                                    with control_dict['lock']:
                                        control_dict['import_complete'] = True
                                        control_dict['end_time'] = dt.now()
                            
                            # Start import thread
                            print(f"[FirnExchange Import] Starting import: {len(selected_files)} files → {exchange_tgt_database}.{exchange_tgt_schema}.{exchange_tgt_table}")
                            # print(f"[FirnExchange Import] Stage: {exchange_stage}, Load Mode: {exchange_load_mode}")
                            # print(f"[FirnExchange Import] Max Workers: {exchange_max_workers}")
                            # print(f"[FirnExchange Import] Log Table: {import_log_table}")
                            
                            import_thread = threading.Thread(
                                target=run_import,
                                args=(exchange_warehouse, selected_files, exchange_max_workers, 
                                      exchange_tgt_database, exchange_tgt_schema, exchange_tgt_table, 
                                      exchange_stage, exchange_load_mode, exchange_control, import_log_table)
                            )
                            import_thread.daemon = True
                            import_thread.start()
                            
                            # print(f"[FirnExchange Import] Import thread started successfully")
                            
                            st.session_state.exchange_migration_running = True
                            st.session_state.exchange_import_has_results = True
                            
                            # Stop execution here to prevent any further processing
                            st.rerun()
                        else:
                            st.error("Files data not available. Please refresh the file list.")
                            st.stop()
            
            with col_btn2:
                if st.button("Stop Execution", disabled=not st.session_state.exchange_migration_running, key="exchange_stop_import", type="secondary"):
                    # Set stop signal (thread-safe)
                    exchange_control = get_exchange_control()
                    with exchange_control['lock']:
                        exchange_control['stop_requested'] = True
                    st.session_state.exchange_migration_running = False
                    st.warning("Stop signal sent. Waiting for active threads to complete...")
                    st.rerun()
        
        # Progress Monitoring (shared for both operations)
        if st.session_state.exchange_migration_running or st.session_state.exchange_tracking_table_name or st.session_state.exchange_import_has_results:
            st.header("Operation Progress")
            
            # Display timing information
            exchange_control = get_exchange_control()
            with exchange_control['lock']:
                start_time = exchange_control.get('start_time')
                end_time = exchange_control.get('end_time')
            
            if start_time:
                time_col1, time_col2, time_col3 = st.columns(3)
                
                with time_col1:
                    st.markdown(f"**Start Time:**  \n{start_time.strftime('%d-%b-%Y %I:%M:%S %p')}")
                
                with time_col2:
                    if end_time:
                        elapsed_seconds = (end_time - start_time).total_seconds()
                    else:
                        elapsed_seconds = (dt.now() - start_time).total_seconds()
                    st.markdown(f"**Elapsed Time:**  \n{format_elapsed_time(elapsed_seconds)}")
                
                with time_col3:
                    if end_time:
                        st.markdown(f"**End Time:**  \n{end_time.strftime('%d-%b-%Y %I:%M:%S %p')}")
                    else:
                        st.markdown(f"**End Time:**  \nIn Progress...")
                
                st.divider()
            
            if operation == "FDN Table Data Export" and st.session_state.exchange_tracking_table_name:
                stats = get_migration_stats(session, st.session_state.exchange_tracking_table_name)
                
                total = sum(stats.values())
                if total > 0:
                    if st.session_state.exchange_migration_running and stats['PENDING'] == 0 and stats['IN_PROGRESS'] == 0:
                        st.session_state.exchange_migration_running = False
                        print("[FirnExchange] Migration detected as complete")
                        # Force UI refresh to update button states
                        st.rerun()
                    
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("Completed", stats['COMPLETED'], f"{stats['COMPLETED']/total*100:.1f}%")
                    with col2:
                        st.metric("In Progress", stats['IN_PROGRESS'])
                    with col3:
                        st.metric("Pending", stats['PENDING'])
                    with col4:
                        st.metric("Failed", stats['FAILED'], delta_color="inverse")
                    
                    progress = stats['COMPLETED'] / total if total > 0 else 0
                    st.progress(progress, text=f"Overall Progress: {progress*100:.1f}%")
                    
                    # Monitoring table - wrapped in collapsible expander (collapsed by default)
                    tracking_data = get_tracking_data(session, st.session_state.exchange_tracking_table_name)
                    
                    if tracking_data:
                        with st.expander(f"📊 Detailed Partition Status ({len(tracking_data)} partitions)", expanded=False):
                            # Display as HTML table to avoid pandas/numpy issues
                            try:
                                # Get column names from first row
                                col_names = list(tracking_data[0].keys())
                                
                                # Build HTML table
                                html = '<div style="max-height: 400px; overflow-y: auto; border: 1px solid #ddd;">'
                                html += '<table style="width: 100%; border-collapse: collapse; font-size: 12px;">'
                                html += '<thead style="position: sticky; top: 0; background: #f0f2f6;"><tr>'
                                for col in col_names:
                                    html += f'<th style="padding: 8px; text-align: left; border-bottom: 2px solid #ddd;">{col}</th>'
                                html += '</tr></thead><tbody>'
                                
                                for row_data in tracking_data:
                                    html += '<tr>'
                                    for col in col_names:
                                        value = row_data.get(col, '')
                                        html += f'<td style="padding: 8px; border-bottom: 1px solid #eee;">{value}</td>'
                                    html += '</tr>'
                                
                                html += '</tbody></table></div>'
                                st.markdown(html, unsafe_allow_html=True)
                            except Exception as e:
                                # Fallback: display as JSON if HTML creation fails
                                print(f"[Tracking Display] HTML table creation failed: {str(e)}, using JSON display")
                                st.caption("📊 Partition Status (JSON view)")
                                st.json(tracking_data)
                    
                    # Auto-refresh while running
                    if st.session_state.exchange_migration_running:
                        time.sleep(60)  # Refresh every 1 minute
                        st.rerun()
    
            elif operation == "Iceberg Table Data Import":
                # Get import progress from control dict
                exchange_control = get_exchange_control()
                
                with exchange_control['lock']:
                    total_files = exchange_control.get('total_files', 0)
                    completed = len(exchange_control.get('completed_files', []))
                    failed = len(exchange_control.get('failed_files', []))
                    import_complete = exchange_control.get('import_complete', False)
                
                # Check if import is complete (either by flag or by file count)
                if st.session_state.exchange_migration_running and (import_complete or (total_files > 0 and completed + failed >= total_files)):
                    st.session_state.exchange_migration_running = False
                    print("[FirnExchange] Import detected as complete")
                    # Reset the completion flag
                    with exchange_control['lock']:
                        exchange_control['import_complete'] = False
                    # Force UI refresh to update button states
                    st.rerun()
                
                if total_files > 0:
                    in_progress = total_files - completed - failed
                    
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("Completed", completed, f"{completed/total_files*100:.1f}%")
                    with col2:
                        st.metric("In Progress", in_progress)
                    with col3:
                        st.metric("Pending", total_files - completed - failed - in_progress)
                    with col4:
                        st.metric("Failed", failed, delta_color="inverse")
                    
                    progress = completed / total_files if total_files > 0 else 0
                    st.progress(progress, text=f"Overall Progress: {progress*100:.1f}%")
                    
                    # Show completed files
                    if completed > 0:
                        with st.expander(f"Completed Files ({completed})"):
                            with exchange_control['lock']:
                                completed_files = exchange_control.get('completed_files', [])
                            for f in completed_files[-10:]:  # Show last 10
                                st.text(f"✅ {f}")
                    
                    # Show failed files
                    if failed > 0:
                        with st.expander(f"Failed Files ({failed})", expanded=True):
                            with exchange_control['lock']:
                                failed_files = exchange_control.get('failed_files', [])
                            for item in failed_files[-10:]:  # Show last 10
                                st.error(f"❌ {item['file']}: {item['error']}")
                    
                    # Auto-refresh while running
                    if st.session_state.exchange_migration_running:
                        time.sleep(60)  # Refresh every 1 minute
                        st.rerun()
    
    else:
        st.info("Please connect to Snowflake using the sidebar")
