"""
Spark HBase Error Handler Module
Handles Py4JJavaError and HBase storage handler issues during Spark SQL operations.

This module provides robust error handling and recovery mechanisms for common
HBase-related errors in Spark environments, particularly addressing:
- org.apache.hadoop.hive.hbase.HBaseStorageHandler loading errors
- Py4J communication issues between Python and JVM
- HBase connection and configuration problems
"""

import logging
import sys
from typing import Optional, Dict, Any, Callable
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HBaseStorageHandlerError(Exception):
    """Custom exception for HBase storage handler related errors."""
    pass


class Py4JSparkError(Exception):
    """Custom exception for Py4J-Spark communication errors."""
    pass


class SparkHBaseErrorHandler:
    """
    Error handler for Spark SQL operations involving HBase tables.
    Provides error detection, logging, and recovery mechanisms.
    """
    
    def __init__(self, enable_fallback: bool = True):
        """
        Initialize the error handler.
        
        Args:
            enable_fallback: Whether to enable fallback mechanisms when errors occur
        """
        self.enable_fallback = enable_fallback
        self.error_count = 0
        self.last_error = None
        
    def check_hbase_dependencies(self) -> Dict[str, bool]:
        """
        Check if required HBase dependencies are available.
        
        Returns:
            Dictionary with availability status of each dependency
        """
        dependencies = {
            'pyspark': False,
            'py4j': False,
            'hbase': False
        }
        
        try:
            import pyspark
            dependencies['pyspark'] = True
            logger.info(f"PySpark version: {pyspark.__version__}")
        except ImportError:
            logger.warning("PySpark not found. Install with: pip install pyspark")
        
        try:
            import py4j
            dependencies['py4j'] = True
            logger.info(f"Py4J version: {py4j.__version__}")
        except ImportError:
            logger.warning("Py4J not found. Install with: pip install py4j")
        
        # Note: HBase Python client check (happybase or similar)
        try:
            import happybase
            dependencies['hbase'] = True
            logger.info("HBase client (happybase) available")
        except ImportError:
            logger.info("HBase Python client not found (optional)")
        
        return dependencies
    
    def handle_py4j_error(self, error: Exception, context: str = "") -> Optional[str]:
        """
        Handle Py4JJavaError with detailed logging and diagnostics.
        
        Args:
            error: The exception that occurred
            context: Additional context about where the error occurred
            
        Returns:
            Error message with diagnostic information
        """
        self.error_count += 1
        self.last_error = error
        
        error_str = str(error)
        error_type = type(error).__name__
        
        logger.error(f"Py4J Error occurred in {context}")
        logger.error(f"Error type: {error_type}")
        logger.error(f"Error message: {error_str}")
        
        # Check for HBase storage handler specific error
        if "HBaseStorageHandler" in error_str:
            return self._handle_hbase_storage_handler_error(error, context)
        
        # Check for general Hive metadata errors
        elif "HiveException" in error_str:
            return self._handle_hive_exception(error, context)
        
        # Generic Py4J error
        else:
            return self._handle_generic_py4j_error(error, context)
    
    def _handle_hbase_storage_handler_error(self, error: Exception, context: str) -> str:
        """
        Handle HBaseStorageHandler loading errors.
        
        Args:
            error: The exception that occurred
            context: Additional context
            
        Returns:
            Diagnostic message and recovery suggestions
        """
        logger.error("HBase Storage Handler Error Detected!")
        logger.error("Root cause: org.apache.hadoop.hive.hbase.HBaseStorageHandler")
        
        diagnostic_msg = f"""
╔══════════════════════════════════════════════════════════════════╗
║           HBase Storage Handler Error Diagnostic                 ║
╚══════════════════════════════════════════════════════════════════╝

ERROR DETAILS:
  Context: {context}
  Error: {str(error)}

ROOT CAUSE:
  The HBase storage handler class cannot be loaded. This typically occurs when:
  1. HBase JAR files are not in the Spark classpath
  2. Hive-HBase connector is missing or incompatible
  3. HBase service is not accessible or properly configured

RECOMMENDED SOLUTIONS:

1. Add HBase JARs to Spark configuration:
   --jars /path/to/hbase-client.jar,/path/to/hbase-common.jar,/path/to/hbase-protocol.jar

2. Configure Spark session with HBase support:
   spark = SparkSession.builder \\
       .config("spark.sql.hive.metastore.jars", "path") \\
       .config("spark.sql.hive.metastore.version", "3.1.2") \\
       .enableHiveSupport() \\
       .getOrCreate()

3. Verify HBase installation and configuration:
   - Check if HBase is running: jps | grep HMaster
   - Verify hbase-site.xml is accessible
   - Ensure Zookeeper quorum is reachable

4. Use alternative approaches:
   - Query HBase directly using happybase or HBase REST API
   - Use Spark's native HBase connector instead of Hive tables
   - Export HBase data to Parquet/ORC and query from HDFS

WORKAROUND:
  If HBase tables must be queried through Hive, ensure the 
  hive-hbase-handler JAR is included in your Spark submit command:
  
  spark-submit --jars hive-hbase-handler-*.jar your_script.py

════════════════════════════════════════════════════════════════════
"""
        
        logger.error(diagnostic_msg)
        
        if self.enable_fallback:
            logger.info("Attempting fallback approach...")
            return self._suggest_fallback_approach()
        
        return diagnostic_msg
    
    def _handle_hive_exception(self, error: Exception, context: str) -> str:
        """
        Handle general HiveException errors.
        
        Args:
            error: The exception that occurred
            context: Additional context
            
        Returns:
            Diagnostic message
        """
        logger.error("Hive Exception Detected!")
        
        diagnostic_msg = f"""
Hive Exception Error:
  Context: {context}
  Error: {str(error)}

Possible causes:
  - Hive metastore connection issues
  - Incompatible Hive version
  - Missing table or database
  - Permission issues

Recommendations:
  1. Verify Hive metastore is running and accessible
  2. Check Hive configuration (hive-site.xml)
  3. Ensure proper Kerberos authentication if security is enabled
  4. Verify table exists: SHOW TABLES;
"""
        
        logger.error(diagnostic_msg)
        return diagnostic_msg
    
    def _handle_generic_py4j_error(self, error: Exception, context: str) -> str:
        """
        Handle generic Py4J errors.
        
        Args:
            error: The exception that occurred
            context: Additional context
            
        Returns:
            Diagnostic message
        """
        logger.error("Generic Py4J Error Detected!")
        
        diagnostic_msg = f"""
Py4J Communication Error:
  Context: {context}
  Error: {str(error)}

Common causes:
  - JVM process terminated unexpectedly
  - Gateway connection lost
  - Java exception not properly wrapped

Recommendations:
  1. Check Spark driver logs for Java stack trace
  2. Increase driver/executor memory if OOM occurred
  3. Verify network connectivity between Python and JVM
  4. Restart Spark session
"""
        
        logger.error(diagnostic_msg)
        return diagnostic_msg
    
    def _suggest_fallback_approach(self) -> str:
        """
        Suggest fallback approaches when HBase access through Spark fails.
        
        Returns:
            Fallback suggestion message
        """
        fallback_msg = """
FALLBACK APPROACHES:

Option 1: Direct HBase Access (Python)
  Use happybase library to query HBase directly:
  
  import happybase
  connection = happybase.Connection('localhost')
  table = connection.table('your_table')
  for key, data in table.scan():
      process(key, data)

Option 2: Export to HDFS
  Use HBase export tool to dump data to HDFS:
  
  hbase org.apache.hadoop.hbase.mapreduce.Export \\
    <table_name> <output_path>
  
  Then query with Spark from HDFS.

Option 3: Use Spark HBase Connector
  Instead of Hive tables, use spark-hbase connector:
  
  df = spark.read.format("org.apache.hadoop.hbase.spark") \\
    .option("hbase.table", "table_name") \\
    .load()
"""
        
        logger.info(fallback_msg)
        return fallback_msg
    
    def safe_execute_sql(self, spark_session, sql_query: str, 
                         error_callback: Optional[Callable] = None) -> Any:
        """
        Safely execute Spark SQL query with error handling.
        
        Args:
            spark_session: Active Spark session
            sql_query: SQL query to execute
            error_callback: Optional callback function for error handling
            
        Returns:
            Query result or None if error occurred
        """
        try:
            logger.info(f"Executing SQL query: {sql_query[:100]}...")
            result = spark_session.sql(sql_query)
            logger.info("Query executed successfully")
            return result
            
        except Exception as e:
            error_type = type(e).__name__
            
            # Check if it's a Py4J error
            if "Py4J" in error_type or "py4j" in str(e).lower():
                self.handle_py4j_error(e, f"SQL execution: {sql_query[:100]}")
            else:
                logger.error(f"Error executing SQL: {error_type}: {str(e)}")
                logger.error(traceback.format_exc())
            
            if error_callback:
                return error_callback(e)
            
            return None
    
    def get_error_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about errors encountered.
        
        Returns:
            Dictionary with error statistics
        """
        return {
            'total_errors': self.error_count,
            'last_error': str(self.last_error) if self.last_error else None,
            'last_error_type': type(self.last_error).__name__ if self.last_error else None
        }


def create_spark_session_with_hbase(app_name: str = "HBaseApp",
                                    hbase_jars: Optional[list] = None,
                                    **spark_configs) -> Any:
    """
    Create a Spark session with HBase support and error handling.
    
    Args:
        app_name: Name for the Spark application
        hbase_jars: List of HBase JAR file paths
        **spark_configs: Additional Spark configuration parameters
        
    Returns:
        Configured Spark session or None if creation fails
    """
    try:
        from pyspark.sql import SparkSession
        
        logger.info(f"Creating Spark session: {app_name}")
        
        builder = SparkSession.builder.appName(app_name)
        
        # Add HBase JARs if provided
        if hbase_jars:
            jars_str = ",".join(hbase_jars)
            builder = builder.config("spark.jars", jars_str)
            logger.info(f"Added HBase JARs: {jars_str}")
        
        # Apply additional configurations
        for key, value in spark_configs.items():
            builder = builder.config(key, value)
        
        # Enable Hive support
        builder = builder.enableHiveSupport()
        
        spark = builder.getOrCreate()
        logger.info("Spark session created successfully")
        
        return spark
        
    except ImportError as e:
        logger.error(f"Failed to import PySpark: {e}")
        logger.error("Install PySpark: pip install pyspark")
        return None
        
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        logger.error(traceback.format_exc())
        return None


# Example usage and testing
if __name__ == "__main__":
    print("=" * 80)
    print("Spark HBase Error Handler - Diagnostic Tool")
    print("=" * 80)
    
    # Initialize error handler
    handler = SparkHBaseErrorHandler(enable_fallback=True)
    
    # Check dependencies
    print("\n1. Checking dependencies...")
    deps = handler.check_hbase_dependencies()
    for dep, available in deps.items():
        status = "✓ Available" if available else "✗ Not found"
        print(f"   {dep}: {status}")
    
    # Simulate HBase storage handler error
    print("\n2. Simulating HBase Storage Handler Error...")
    try:
        # Simulate the actual error from the problem statement
        error_msg = """py4j.protocol.Py4JJavaError: An error occurred while calling o113.sql.
: java.lang.RuntimeException: org.apache.hadoop.hive.ql.metadata.HiveException: 
Error in loading storage handler.org.apache.hadoop.hive.hbase.HBaseStorageHandler"""
        
        raise Exception(error_msg)
        
    except Exception as e:
        handler.handle_py4j_error(e, "Test SQL execution")
    
    # Show statistics
    print("\n3. Error Statistics:")
    stats = handler.get_error_statistics()
    for key, value in stats.items():
        print(f"   {key}: {value}")
    
    print("\n" + "=" * 80)
    print("Diagnostic complete. See logs above for detailed recommendations.")
    print("=" * 80)
