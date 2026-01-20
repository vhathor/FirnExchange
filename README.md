# FirnExchange User Guide

*A Simple Guide to High-Performance Data Migration Between Snowflake Tables*

---

## What is FirnExchange?

FirnExchange is a Streamlit application that enables high-performance migration of large datasets between Snowflake FDN tables and Iceberg tables using external cloud stages (Azure Blob Storage, AWS S3, or GCS). It uses parallel processing to handle millions or billions of rows efficiently by partitioning data into manageable chunks.

## Prerequisites

Before you start, ensure you have:

- ✅ A Snowflake account with appropriate permissions
- ✅ An external stage configured (Azure Blob, AWS S3, or GCS)
- ✅ Source table (FDN or Iceberg) ready for migration
- ✅ Target Iceberg table created (or FirnExchange can create it for you)
- ✅ A warehouse for executing queries

## Getting Started

### Step 1: Launch the Application

Open FirnExchange in Snowflake (Streamlit in Snowflake environment). The app will automatically connect using your Snowflake session.

### Step 2: Select Your Warehouse

Choose a warehouse from the dropdown in the sidebar. This warehouse will execute all migration operations.

---

## Export Flow: FDN Table → Stage → Iceberg Table

**Use Case**: Migrate data from a standard Snowflake table to an Iceberg table via an external stage.

### 1. Configure Export Settings

In the **Data Export (FDN → Stage)** section:

- **Select Database**: Choose the database containing your source table
- **Select Schema**: Choose the schema
- **Select Source Table**: Pick the FDN table you want to export
- **Select External Stage**: Choose your configured stage (e.g., Azure Blob Storage)
- **Stage Path**: Enter a unique path for this export (e.g., `export_2026_01/`)

### 2. Analyze Partitions

- **Select Partition Key Column**: Choose ONE column to partition your data (typically a date column)
- Click **"Analyze Partitions"**

FirnExchange will analyze your table and display all unique partition values with row counts.

### 3. Select Partitions to Export

You have three options:

- **Select All**: Export all partitions
- **Select Non-Completed**: Export only partitions that haven't been successfully exported yet
- **Manual Selection**: Expand the partition table and check specific partitions

💡 **Tip**: For initial runs, start with a few partitions to test, then use "Select Non-Completed" for subsequent runs.

### 4. Configure Export Options

- **Max Threads**: Set the number of parallel export operations (default: 4)
  - More threads = faster migration
  - Don't exceed your warehouse capacity
- **File Size (GB)**: Maximum size per Parquet file (default: 5 GB)

### 5. Start Export

Click **"Start Export"** to begin the migration. 

Monitor progress in the **Operation Progress** section. The status updates every minute showing:
- Completed partitions
- In-progress partitions
- Failed partitions (if any)
- Overall completion percentage

### 6. Troubleshooting Failed Partitions

If any partitions fail:
1. Expand the **Detailed Partition Status** section
2. Check the error message for the failed partition
3. Fix the issue (e.g., adjust file size, check warehouse capacity)
4. Use "Select Non-Completed" to retry only failed partitions

---

## Import Flow: Stage → Iceberg Table

**Use Case**: Load data from an external stage into an Iceberg table.

### 1. Configure Import Settings

In the **Data Import (Stage → Iceberg)** section:

- **Select Database**: Choose the target database
- **Select Schema**: Choose the target schema
- **Select Target Iceberg Table**: Choose the destination table
- **Select External Stage**: Choose the stage containing your data
- **Stage Path**: Enter the path where files are located (same as export path)

### 2. Analyze Files

Click **"Analyze Files in Stage"**

FirnExchange will scan the stage path and list all Parquet files ready for import.

### 3. Select Files to Import

Similar to export:

- **Select All Files**: Import all files from the stage
- **Select Non-Completed**: Import only files not yet loaded
- **Manual Selection**: Check specific files to import

### 4. Configure Import Options

- **Max Threads**: Number of parallel import operations (default: 4)

### 5. Start Import

Click **"Start Import"** to begin loading data into your Iceberg table.

Monitor progress in real-time, with updates every minute showing completion status.

---

## Best Practices

### 🎯 Performance Tips

1. **Choose the Right Partition Key**: Use a column with good data distribution (e.g., date columns)
2. **Tune Thread Count**: Start with 4 threads, increase based on warehouse size and performance
3. **Monitor Warehouse Load**: Ensure your warehouse can handle the thread count
4. **Incremental Migration**: Use "Select Non-Completed" for large tables to migrate in batches

### ⚠️ Important Notes

- **Tracking Tables**: FirnExchange creates log tables (`*_EXPORT_FELOG` and `*_IMPORT_FELOG`) to track progress
- **Resume Capability**: If a migration is interrupted, simply rerun and use "Select Non-Completed"
- **Drop Log Tables**: Use "Drop Log Table" buttons to start fresh analysis
- **Single Selection Only**: Partition key must be a single column

### 🔍 Monitoring Your Migration

1. **Progress Section**: Shows real-time summary (updates every 60 seconds)
2. **Partition Status**: Expand to see detailed status of each partition/file
3. **Snowflake Query History**: Check for detailed execution logs and errors

---

## Common Scenarios

### Scenario 1: Migrating a 10 Billion Row Table

1. Choose a date partition key with ~daily granularity
2. Analyze partitions (might show 365+ partitions)
3. Test with 10 partitions first
4. Once successful, use "Select Non-Completed" to process remaining partitions
5. Use 8-10 threads on a Medium or Large warehouse

### Scenario 2: Failed Partitions

1. Check the error in Detailed Partition Status
2. Common fixes:
   - Reduce file size if files are too large
   - Increase warehouse size if timeout occurs
   - Check stage permissions if access errors occur
3. Click "Select Non-Completed" and re-run

### Scenario 3: Incremental Daily Updates

1. Keep the same log table
2. New partitions will automatically appear when you re-analyze
3. Select only new partition values
4. Export and import as usual

---

## Getting Help

If you encounter issues:

1. Check the **Detailed Partition Status** for error messages
2. Review Snowflake Query History for detailed execution logs
3. Verify stage permissions and warehouse capacity
4. Consult the deployment documentation for setup requirements

---

## Summary

FirnExchange makes large-scale data migration simple:

1. **Export**: Partition → Export to Stage (parallel)
2. **Import**: Load from Stage → Iceberg Table (parallel)
3. **Monitor**: Track progress in real-time
4. **Resume**: Handle failures gracefully with retry capability

With proper configuration, you can migrate billions of rows efficiently while maintaining full control over the process.

Happy Migrating! ❄️🚀
