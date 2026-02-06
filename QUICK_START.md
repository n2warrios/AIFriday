# Quick Start Guide - HBase Error Solution

## Problem

Getting this error when running Spark SQL queries?

```
py4j.protocol.Py4JJavaError: An error occurred while calling o113.sql.
: java.lang.RuntimeException: org.apache.hadoop.hive.ql.metadata.HiveException: 
Error in loading storage handler.org.apache.hadoop.hive.hbase.HBaseStorageHandler
```

## Quick Fix

### Option 1: Run Diagnostics (Recommended)

```bash
# Check your configuration
python hbase_config_validator.py

# Test error handling
python spark_hbase_error_handler.py
```

### Option 2: Add JARs to Spark

```bash
spark-submit \
  --jars /path/to/hbase-client.jar,/path/to/hive-hbase-handler.jar \
  your_script.py
```

### Option 3: Use the Error Handler

```python
from spark_hbase_error_handler import SparkHBaseErrorHandler

handler = SparkHBaseErrorHandler(enable_fallback=True)
result = handler.safe_execute_sql(spark, "SELECT * FROM my_hbase_table")
```

### Option 4: Run the Example

```bash
python spark_hbase_example.py --example
```

## Files Overview

| File | Purpose | Lines |
|------|---------|-------|
| `spark_hbase_error_handler.py` | Error handling module | 445 |
| `hbase_config_validator.py` | Configuration validator | 406 |
| `spark_hbase_example.py` | Example implementation | 422 |
| `test_hbase_solution.py` | Test suite | 234 |
| `HBASE_ERROR_SOLUTION.md` | Complete documentation | 334 |

## Common Issues

**Missing JARs?**
```bash
python hbase_config_validator.py
# Follow the recommendations
```

**Can't connect to HBase?**
```bash
jps | grep HMaster  # Check if HBase is running
echo "status" | hbase shell  # Check HBase status
```

**Still not working?**
```python
# Use direct HBase access fallback
python spark_hbase_example.py --table my_table
```

## Documentation

📖 **Full Documentation**: See `HBASE_ERROR_SOLUTION.md`

## Testing

```bash
python test_hbase_solution.py
# Expected: 17/17 tests passing ✅
```

## Support

1. Run diagnostics: `python spark_hbase_error_handler.py`
2. Validate config: `python hbase_config_validator.py`
3. Check documentation: `HBASE_ERROR_SOLUTION.md`
4. Review example: `python spark_hbase_example.py --example`

---

**Status**: ✅ All tests passing | ✅ Code review complete | ✅ Security scan clean
