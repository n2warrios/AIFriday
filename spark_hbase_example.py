"""
Example: Safe Spark SQL Execution with HBase Tables
Demonstrates how to safely query HBase tables through Spark SQL with proper error handling.

This example shows:
1. Creating a Spark session with HBase support
2. Using the error handler to catch and handle Py4J errors
3. Implementing fallback strategies
4. Querying HBase tables safely
"""

import sys
import logging
from typing import Optional, Any

# Import our error handler
from spark_hbase_error_handler import (
    SparkHBaseErrorHandler, 
    create_spark_session_with_hbase
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SafeHBaseQuerier:
    """Safe HBase query executor with error handling."""
    
    def __init__(self, app_name: str = "SafeHBaseApp"):
        """
        Initialize the safe querier.
        
        Args:
            app_name: Name for the Spark application
        """
        self.app_name = app_name
        self.spark = None
        self.error_handler = SparkHBaseErrorHandler(enable_fallback=True)
        
    def initialize_spark(self, hbase_jars: Optional[list] = None) -> bool:
        """
        Initialize Spark session with HBase support.
        
        Args:
            hbase_jars: List of HBase JAR file paths
            
        Returns:
            True if initialization successful, False otherwise
        """
        logger.info("Initializing Spark session...")
        
        # Check dependencies first
        deps = self.error_handler.check_hbase_dependencies()
        if not deps['pyspark']:
            logger.error("PySpark not available. Cannot proceed.")
            return False
        
        # Create Spark session
        self.spark = create_spark_session_with_hbase(
            app_name=self.app_name,
            hbase_jars=hbase_jars,
            **{
                "spark.sql.hive.metastore.version": "3.1.2",
                "spark.executor.memory": "4g",
                "spark.driver.memory": "2g"
            }
        )
        
        if self.spark is None:
            logger.error("Failed to create Spark session")
            return False
        
        logger.info("Spark session initialized successfully")
        return True
    
    def create_hbase_table_mapping(self, hive_table_name: str,
                                   hbase_table_name: str,
                                   columns_mapping: dict) -> bool:
        """
        Create Hive external table mapping to HBase table.
        
        Args:
            hive_table_name: Name of the Hive table to create
            hbase_table_name: Name of the HBase table to map to
            columns_mapping: Dictionary of column mappings
            
        Returns:
            True if successful, False otherwise
        """
        if not self.spark:
            logger.error("Spark session not initialized")
            return False
        
        # Build column definitions
        column_defs = []
        hbase_columns = []
        
        for hive_col, hbase_col in columns_mapping.items():
            column_defs.append(f"{hive_col} STRING")
            hbase_columns.append(f"'{hive_col}'='{hbase_col}'")
        
        # Create DDL statement
        ddl = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {hive_table_name} (
            {', '.join(column_defs)}
        )
        STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
        WITH SERDEPROPERTIES (
            'hbase.columns.mapping' = ':key,{",".join(hbase_columns)}'
        )
        TBLPROPERTIES (
            'hbase.table.name' = '{hbase_table_name}'
        )
        """
        
        logger.info(f"Creating table mapping: {hive_table_name} -> {hbase_table_name}")
        
        # Execute with error handling
        result = self.error_handler.safe_execute_sql(
            self.spark,
            ddl,
            error_callback=self._handle_table_creation_error
        )
        
        return result is not None
    
    def query_hbase_table(self, query: str, limit: int = 100) -> Optional[Any]:
        """
        Query HBase table through Spark SQL with error handling.
        
        Args:
            query: SQL query to execute
            limit: Maximum number of rows to return
            
        Returns:
            DataFrame with results or None if error occurred
        """
        if not self.spark:
            logger.error("Spark session not initialized")
            return None
        
        # Add LIMIT if not present
        if "LIMIT" not in query.upper():
            query = f"{query} LIMIT {limit}"
        
        logger.info(f"Executing query: {query}")
        
        # Execute with error handling
        result = self.error_handler.safe_execute_sql(
            self.spark,
            query,
            error_callback=self._handle_query_error
        )
        
        if result is not None:
            try:
                # Show results
                logger.info("Query results:")
                result.show(truncate=False)
                return result
            except Exception as e:
                logger.error(f"Error displaying results: {e}")
                return None
        
        return None
    
    def _handle_table_creation_error(self, error: Exception) -> None:
        """
        Handle errors during table creation.
        
        Args:
            error: The exception that occurred
        """
        logger.error("Table creation failed!")
        logger.error("This is likely due to:")
        logger.error("  1. HBase storage handler not available")
        logger.error("  2. HBase table doesn't exist")
        logger.error("  3. Insufficient permissions")
        
        logger.info("Suggested workarounds:")
        logger.info("  1. Verify HBase table exists: echo 'list' | hbase shell")
        logger.info("  2. Check HBase permissions")
        logger.info("  3. Use direct HBase access instead of Hive tables")
        
        return None
    
    def _handle_query_error(self, error: Exception) -> None:
        """
        Handle errors during query execution.
        
        Args:
            error: The exception that occurred
        """
        logger.error("Query execution failed!")
        
        # Try to suggest alternative approach
        logger.info("Attempting alternative query method...")
        
        return None
    
    def use_direct_hbase_access(self, table_name: str, 
                                row_key: str = None) -> Optional[dict]:
        """
        Fallback method: Direct HBase access using happybase.
        
        Args:
            table_name: HBase table name
            row_key: Optional specific row key to fetch
            
        Returns:
            Dictionary with results or None
        """
        try:
            import happybase
            
            logger.info("Using direct HBase access (fallback method)")
            logger.info(f"Connecting to HBase table: {table_name}")
            
            # Connect to HBase
            connection = happybase.Connection('localhost')  # Adjust host as needed
            table = connection.table(table_name)
            
            results = []
            
            if row_key:
                # Fetch specific row
                row = table.row(row_key.encode())
                if row:
                    results.append({
                        'row_key': row_key,
                        'data': row
                    })
            else:
                # Scan table (limited rows)
                for key, data in table.scan(limit=100):
                    results.append({
                        'row_key': key.decode(),
                        'data': data
                    })
            
            connection.close()
            
            logger.info(f"Retrieved {len(results)} rows from HBase")
            return {
                'success': True,
                'rows': results,
                'method': 'direct_hbase'
            }
            
        except ImportError:
            logger.error("happybase not installed")
            logger.info("Install with: pip install happybase")
            return None
        except Exception as e:
            logger.error(f"Direct HBase access failed: {e}")
            return None
    
    def cleanup(self):
        """Clean up resources."""
        if self.spark:
            logger.info("Stopping Spark session...")
            self.spark.stop()
            logger.info("Spark session stopped")
        
        # Print error statistics
        stats = self.error_handler.get_error_statistics()
        if stats['total_errors'] > 0:
            logger.info(f"Total errors encountered: {stats['total_errors']}")


def example_migration_query():
    """
    Example: Query HBase table for Teradata-to-Hadoop migration.
    This demonstrates a real-world use case from the migration project.
    """
    print("\n" + "=" * 80)
    print("Example: Teradata to Hadoop Migration - HBase Query")
    print("=" * 80 + "\n")
    
    # Initialize querier
    querier = SafeHBaseQuerier(app_name="TeradataToHadoopMigration")
    
    # Define HBase JAR paths (adjust to your environment)
    hbase_jars = [
        # Example paths - adjust to your actual environment
        # "/opt/cloudera/parcels/CDH/jars/hbase-client.jar",
        # "/opt/cloudera/parcels/CDH/jars/hbase-common.jar",
        # "/opt/cloudera/parcels/CDH/jars/hive-hbase-handler.jar",
    ]
    
    # Initialize Spark
    if not querier.initialize_spark(hbase_jars):
        print("❌ Failed to initialize Spark. Check your configuration.")
        return
    
    print("\n1️⃣  Creating HBase table mapping...")
    
    # Example: Map customer data from HBase
    columns = {
        'customer_id': 'cf:customer_id',
        'customer_name': 'cf:name',
        'account_balance': 'cf:balance',
        'last_updated': 'cf:updated'
    }
    
    success = querier.create_hbase_table_mapping(
        hive_table_name='customer_data_hbase',
        hbase_table_name='customers',
        columns_mapping=columns
    )
    
    if not success:
        print("⚠️  Table mapping failed. Attempting fallback method...")
        
        # Try direct HBase access
        results = querier.use_direct_hbase_access('customers')
        
        if results and results['success']:
            print(f"✅ Retrieved {len(results['rows'])} rows using direct HBase access")
            
            # Display sample rows
            for i, row in enumerate(results['rows'][:5], 1):
                print(f"\nRow {i}:")
                print(f"  Key: {row['row_key']}")
                print(f"  Data: {row['data']}")
        else:
            print("❌ Both Spark SQL and direct HBase access failed")
    else:
        print("✅ Table mapping created successfully")
        
        print("\n2️⃣  Querying customer data...")
        
        # Query the mapped table
        query = """
        SELECT customer_id, customer_name, account_balance
        FROM customer_data_hbase
        WHERE account_balance > '1000'
        """
        
        result_df = querier.query_hbase_table(query)
        
        if result_df is None:
            print("⚠️  Query failed. Check error logs above.")
    
    # Cleanup
    print("\n3️⃣  Cleaning up...")
    querier.cleanup()
    
    print("\n" + "=" * 80)
    print("Example complete!")
    print("=" * 80 + "\n")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Safe HBase query executor with error handling"
    )
    parser.add_argument(
        '--example',
        action='store_true',
        help='Run example migration query'
    )
    parser.add_argument(
        '--query',
        help='SQL query to execute',
        default=None
    )
    parser.add_argument(
        '--table',
        help='HBase table name for direct access',
        default=None
    )
    
    args = parser.parse_args()
    
    if args.example:
        # Run example
        example_migration_query()
    elif args.query or args.table:
        # Custom query or table access
        querier = SafeHBaseQuerier()
        
        if not querier.initialize_spark():
            print("Failed to initialize Spark")
            sys.exit(1)
        
        if args.query:
            querier.query_hbase_table(args.query)
        elif args.table:
            results = querier.use_direct_hbase_access(args.table)
            if results:
                print(f"Retrieved {len(results.get('rows', []))} rows")
        
        querier.cleanup()
    else:
        # Show help
        parser.print_help()
        print("\nExamples:")
        print("  python spark_hbase_example.py --example")
        print("  python spark_hbase_example.py --query 'SELECT * FROM my_hbase_table'")
        print("  python spark_hbase_example.py --table my_table")


if __name__ == "__main__":
    main()
