# HBase Storage Handler Error Solution

## Problem Overview

When executing Spark SQL queries against HBase tables, you may encounter the following error:

```
py4j.protocol.Py4JJavaError: An error occurred while calling o113.sql.
: java.lang.RuntimeException: org.apache.hadoop.hive.ql.metadata.HiveException: 
Error in loading storage handler.org.apache.hadoop.hive.hbase.HBaseStorageHandler
```

### Root Cause

This error occurs when:
1. **HBase JAR files** are not in the Spark classpath
2. **Hive-HBase connector** (`hive-hbase-handler.jar`) is missing or incompatible
3. **HBase service** is not accessible or properly configured
4. **Configuration files** (hbase-site.xml) are not available to Spark

## Solution Components

This repository provides a comprehensive solution with three main components:

### 1. Error Handler (`spark_hbase_error_handler.py`)

A robust error handling module that:
- ✅ Detects and classifies Py4J errors
- ✅ Provides detailed diagnostic information
- ✅ Suggests specific fixes for HBase storage handler errors
- ✅ Implements fallback strategies
- ✅ Logs all errors with context

**Key Features:**
- `SparkHBaseErrorHandler` class for comprehensive error handling
- Automatic error classification (HBase, Hive, Py4J)
- Built-in dependency checking
- Safe SQL execution with automatic error recovery
- Detailed diagnostic messages with actionable recommendations

### 2. Configuration Validator (`hbase_config_validator.py`)

A validation tool that checks your HBase setup:
- ✅ Validates environment variables (HBASE_HOME, HADOOP_HOME, etc.)
- ✅ Checks for required JAR files
- ✅ Verifies configuration files (hbase-site.xml, etc.)
- ✅ Inspects classpath settings
- ✅ Generates spark-submit commands with correct configuration

**Usage:**
```bash
# Run full validation
python hbase_config_validator.py

# Specify custom HBASE_HOME
python hbase_config_validator.py --hbase-home /opt/hbase

# Generate spark-submit command
python hbase_config_validator.py --generate-command my_script.py --jars /path/to/jars
```

### 3. Example Implementation (`spark_hbase_example.py`)

A complete example demonstrating:
- ✅ Safe Spark session creation with HBase support
- ✅ Creating Hive external tables mapped to HBase
- ✅ Querying HBase tables through Spark SQL
- ✅ Fallback to direct HBase access when needed
- ✅ Real-world migration scenario (Teradata to Hadoop)

**Usage:**
```bash
# Run the example migration scenario
python spark_hbase_example.py --example

# Execute custom query
python spark_hbase_example.py --query "SELECT * FROM my_hbase_table LIMIT 10"

# Direct HBase table access (fallback)
python spark_hbase_example.py --table my_table
```

## Quick Start

### Step 1: Run Diagnostics

First, validate your HBase configuration:

```bash
python hbase_config_validator.py
```

This will check:
- Environment variables
- Required JAR files
- Configuration files
- Classpath settings

### Step 2: Address Issues

Follow the recommendations from the validator output. Common fixes:

**Missing JARs:**
```bash
export SPARK_HOME=/path/to/spark
cp $HBASE_HOME/lib/hbase-*.jar $SPARK_HOME/jars/
cp $HIVE_HOME/lib/hive-hbase-handler-*.jar $SPARK_HOME/jars/
```

**Missing Configuration:**
```bash
cp $HBASE_HOME/conf/hbase-site.xml $SPARK_HOME/conf/
```

**Environment Variables:**
```bash
export HBASE_HOME=/opt/hbase
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$HBASE_HOME/lib/*
```

### Step 3: Use the Error Handler

In your Spark application, use the error handler:

```python
from spark_hbase_error_handler import SparkHBaseErrorHandler, create_spark_session_with_hbase

# Initialize error handler
handler = SparkHBaseErrorHandler(enable_fallback=True)

# Create Spark session
spark = create_spark_session_with_hbase(
    app_name="MyApp",
    hbase_jars=[
        "/path/to/hbase-client.jar",
        "/path/to/hive-hbase-handler.jar"
    ]
)

# Execute SQL safely
result = handler.safe_execute_sql(
    spark,
    "SELECT * FROM my_hbase_table LIMIT 10"
)
```

### Step 4: Handle Errors Gracefully

If errors occur, the handler provides detailed diagnostics:

```python
try:
    result = spark.sql("SELECT * FROM hbase_table")
except Exception as e:
    # Get detailed error analysis
    handler.handle_py4j_error(e, "HBase table query")
    # Will print:
    # - Error classification
    # - Root cause analysis
    # - Specific fix recommendations
    # - Fallback approaches
```

## Detailed Solutions

### Solution 1: Add JARs to Spark Submit

The most common solution is to include required JARs:

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --jars /opt/hbase/lib/hbase-client.jar,\
/opt/hbase/lib/hbase-common.jar,\
/opt/hbase/lib/hbase-protocol.jar,\
/opt/hive/lib/hive-hbase-handler.jar \
  --conf spark.sql.hive.metastore.version=3.1.2 \
  --conf spark.executor.memory=4g \
  your_script.py
```

### Solution 2: Configure Spark Session Properly

In your Python code:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HBaseApp") \
    .config("spark.jars", "/path/to/hbase-jars/*.jar") \
    .config("spark.sql.hive.metastore.version", "3.1.2") \
    .config("spark.sql.hive.metastore.jars", "path") \
    .enableHiveSupport() \
    .getOrCreate()
```

### Solution 3: Use Direct HBase Access (Fallback)

If Spark SQL integration fails, use direct HBase access:

```python
import happybase

# Connect to HBase
connection = happybase.Connection('localhost')
table = connection.table('my_table')

# Scan table
for key, data in table.scan(limit=100):
    print(f"Row: {key}, Data: {data}")

connection.close()
```

Install happybase:
```bash
pip install happybase
```

### Solution 4: Export HBase to HDFS

For large-scale data access:

```bash
# Export HBase table to HDFS
hbase org.apache.hadoop.hbase.mapreduce.Export \
  my_table \
  /user/data/my_table_export

# Query from HDFS with Spark
df = spark.read.parquet("/user/data/my_table_export")
df.createOrReplaceTempView("my_table_view")
spark.sql("SELECT * FROM my_table_view")
```

## Common Error Scenarios

### Scenario 1: Missing hive-hbase-handler.jar

**Error:**
```
Error in loading storage handler.org.apache.hadoop.hive.hbase.HBaseStorageHandler
```

**Fix:**
```bash
# Download from Maven or copy from Hive installation
cp $HIVE_HOME/lib/hive-hbase-handler-*.jar $SPARK_HOME/jars/
```

### Scenario 2: HBase Configuration Not Found

**Error:**
```
Could not initialize class org.apache.hadoop.hbase.HBaseConfiguration
```

**Fix:**
```bash
# Copy HBase configuration to Spark
cp $HBASE_HOME/conf/hbase-site.xml $SPARK_HOME/conf/
```

### Scenario 3: Version Incompatibility

**Error:**
```
IncompatibleClassChangeError
```

**Fix:**
Ensure HBase, Hive, and Spark versions are compatible. Check compatibility matrix:
- HBase 2.x requires Hadoop 3.x
- Hive 3.x requires specific HBase connector version

## Testing the Solution

### Test 1: Run Diagnostics
```bash
python spark_hbase_error_handler.py
```

Expected output: Dependency check, error simulation, and diagnostic recommendations.

### Test 2: Validate Configuration
```bash
python hbase_config_validator.py
```

Expected output: Validation report with status of all components.

### Test 3: Run Example
```bash
python spark_hbase_example.py --example
```

Expected output: Example migration query with error handling demonstration.

## Integration with Teradata-to-Hadoop Migration

This solution is particularly useful for the Teradata to Hadoop migration project mentioned in the repository's meeting transcripts. The migration involves:

1. **Data Migration**: Moving data from Teradata to Hadoop/HBase
2. **Query Migration**: Converting BTEQ scripts to Spark SQL
3. **Validation**: Ensuring 100% parity between systems

### Migration Workflow

```python
from spark_hbase_example import SafeHBaseQuerier

# Initialize querier
querier = SafeHBaseQuerier(app_name="TeradataToHadoopMigration")

# Map migrated table
querier.create_hbase_table_mapping(
    hive_table_name='customer_data',
    hbase_table_name='migrated_customers',
    columns_mapping={
        'customer_id': 'cf:id',
        'customer_name': 'cf:name',
        'balance': 'cf:balance'
    }
)

# Query with validation
result = querier.query_hbase_table(
    "SELECT * FROM customer_data WHERE balance > 1000"
)

# Cleanup
querier.cleanup()
```

## Requirements

### Python Dependencies

```bash
pip install pyspark==3.3.2
pip install py4j==0.10.9.5
pip install happybase  # Optional, for direct HBase access
```

### Java/Hadoop Dependencies

Required JAR files:
- `hbase-client-*.jar`
- `hbase-common-*.jar`
- `hbase-protocol-*.jar`
- `hbase-server-*.jar`
- `hive-hbase-handler-*.jar`
- `hbase-hadoop-compat-*.jar`

Configuration files:
- `hbase-site.xml`
- `core-site.xml`
- `hdfs-site.xml`
- `hive-site.xml`

## Troubleshooting

### Issue: "Cannot find JAR files"

**Solution:** Use the validator to locate JAR files:
```bash
python hbase_config_validator.py
```

### Issue: "Connection refused to HBase"

**Solution:** Verify HBase is running:
```bash
jps | grep HMaster
echo "status" | hbase shell
```

### Issue: "Permission denied"

**Solution:** Check Kerberos authentication:
```bash
kinit -kt /path/to/keytab user@REALM
```

### Issue: "Py4J gateway terminated"

**Solution:** Increase driver memory:
```bash
--conf spark.driver.memory=4g
```

## Additional Resources

- [Apache HBase Documentation](https://hbase.apache.org/book.html)
- [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Hive-HBase Integration](https://cwiki.apache.org/confluence/display/Hive/HBaseIntegration)

## License

This solution is provided as-is for the AIFriday migration project.

## Support

For issues or questions:
1. Run the diagnostic tool: `python spark_hbase_error_handler.py`
2. Check the validation report: `python hbase_config_validator.py`
3. Review error logs with detailed recommendations
4. Consult the example implementation for reference

---

**Last Updated:** 2026-02-06
**Version:** 1.0.0
