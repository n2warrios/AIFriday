"""
Test suite for HBase error handling modules
Tests the error handler, validator, and example implementations.
"""

import sys
import unittest
from io import StringIO


class TestHBaseErrorHandler(unittest.TestCase):
    """Test cases for SparkHBaseErrorHandler."""
    
    def setUp(self):
        """Set up test fixtures."""
        from spark_hbase_error_handler import SparkHBaseErrorHandler
        self.handler = SparkHBaseErrorHandler(enable_fallback=True)
    
    def test_initialization(self):
        """Test handler initialization."""
        self.assertIsNotNone(self.handler)
        self.assertEqual(self.handler.error_count, 0)
        self.assertTrue(self.handler.enable_fallback)
    
    def test_check_dependencies(self):
        """Test dependency checking."""
        deps = self.handler.check_hbase_dependencies()
        self.assertIn('pyspark', deps)
        self.assertIn('py4j', deps)
        self.assertIn('hbase', deps)
        self.assertIsInstance(deps['pyspark'], bool)
    
    def test_hbase_storage_handler_error(self):
        """Test HBase storage handler error detection."""
        error_msg = """py4j.protocol.Py4JJavaError: An error occurred while calling o113.sql.
: java.lang.RuntimeException: org.apache.hadoop.hive.ql.metadata.HiveException: 
Error in loading storage handler.org.apache.hadoop.hive.hbase.HBaseStorageHandler"""
        
        test_error = Exception(error_msg)
        result = self.handler.handle_py4j_error(test_error, "Test context")
        
        self.assertIsNotNone(result)
        self.assertEqual(self.handler.error_count, 1)
        # Result should contain either diagnostic info or fallback approaches
        self.assertTrue("HBase" in result or "hbase" in result.lower())
        self.assertTrue("fallback" in result.lower() or "option" in result.lower())
    
    def test_hive_exception_error(self):
        """Test Hive exception error detection."""
        error_msg = "HiveException: Table not found"
        test_error = Exception(error_msg)
        result = self.handler.handle_py4j_error(test_error, "Query execution")
        
        self.assertIsNotNone(result)
        self.assertIn("Hive", result)
    
    def test_generic_py4j_error(self):
        """Test generic Py4J error handling."""
        error_msg = "Py4J gateway connection error"
        test_error = Exception(error_msg)
        result = self.handler.handle_py4j_error(test_error, "Connection test")
        
        self.assertIsNotNone(result)
    
    def test_error_statistics(self):
        """Test error statistics tracking."""
        # Generate some errors
        error1 = Exception("Test error 1")
        error2 = Exception("Test error 2")
        
        self.handler.handle_py4j_error(error1, "Context 1")
        self.handler.handle_py4j_error(error2, "Context 2")
        
        stats = self.handler.get_error_statistics()
        self.assertEqual(stats['total_errors'], 2)
        self.assertIsNotNone(stats['last_error'])
        self.assertEqual(stats['last_error_type'], 'Exception')


class TestHBaseConfigValidator(unittest.TestCase):
    """Test cases for HBaseConfigValidator."""
    
    def setUp(self):
        """Set up test fixtures."""
        from hbase_config_validator import HBaseConfigValidator
        self.validator = HBaseConfigValidator()
    
    def test_initialization(self):
        """Test validator initialization."""
        self.assertIsNotNone(self.validator)
        self.assertEqual(len(self.validator.issues), 0)
        self.assertEqual(len(self.validator.warnings), 0)
    
    def test_check_environment(self):
        """Test environment variable checking."""
        env_vars = self.validator._check_environment()
        
        self.assertIn('HBASE_HOME', env_vars)
        self.assertIn('HADOOP_HOME', env_vars)
        self.assertIn('SPARK_HOME', env_vars)
        self.assertIn('JAVA_HOME', env_vars)
    
    def test_check_jars(self):
        """Test JAR file checking."""
        jars = self.validator._check_jars()
        
        self.assertIn('found', jars)
        self.assertIn('missing', jars)
        self.assertIsInstance(jars['found'], list)
        self.assertIsInstance(jars['missing'], list)
    
    def test_check_config_files(self):
        """Test configuration file checking."""
        config_files = self.validator._check_config_files()
        
        self.assertIn('hbase-site.xml', config_files)
        self.assertIn('core-site.xml', config_files)
        self.assertIsInstance(config_files['hbase-site.xml'], bool)
    
    def test_check_classpath(self):
        """Test classpath checking."""
        classpath = self.validator._check_classpath()
        
        self.assertIn('hadoop_classpath_set', classpath)
        self.assertIn('contains_hbase', classpath)
        self.assertIsInstance(classpath['hadoop_classpath_set'], bool)
    
    def test_validate_all(self):
        """Test complete validation."""
        results = self.validator.validate_all()
        
        self.assertIn('environment', results)
        self.assertIn('jars', results)
        self.assertIn('config_files', results)
        self.assertIn('classpath', results)
        self.assertIn('issues', results)
        self.assertIn('warnings', results)
        self.assertIn('recommendations', results)
    
    def test_generate_spark_submit_command(self):
        """Test spark-submit command generation."""
        cmd = self.validator.generate_spark_submit_command(
            script_path="/path/to/script.py",
            jar_paths=["/path/to/jar1.jar", "/path/to/jar2.jar"]
        )
        
        self.assertIsNotNone(cmd)
        self.assertIn("spark-submit", cmd)
        self.assertIn("script.py", cmd)
        self.assertIn("jar1.jar", cmd)


class TestIntegration(unittest.TestCase):
    """Integration tests for the complete solution."""
    
    def test_modules_importable(self):
        """Test that all modules can be imported."""
        try:
            import spark_hbase_error_handler
            import hbase_config_validator
            import spark_hbase_example
            success = True
        except ImportError as e:
            success = False
            self.fail(f"Failed to import modules: {e}")
        
        self.assertTrue(success)
    
    def test_error_handler_instantiation(self):
        """Test error handler can be instantiated."""
        from spark_hbase_error_handler import SparkHBaseErrorHandler
        handler = SparkHBaseErrorHandler()
        self.assertIsNotNone(handler)
    
    def test_validator_instantiation(self):
        """Test validator can be instantiated."""
        from hbase_config_validator import HBaseConfigValidator
        validator = HBaseConfigValidator()
        self.assertIsNotNone(validator)
    
    def test_safe_querier_instantiation(self):
        """Test safe querier can be instantiated."""
        from spark_hbase_example import SafeHBaseQuerier
        querier = SafeHBaseQuerier()
        self.assertIsNotNone(querier)


def run_tests():
    """Run all tests and display results."""
    print("=" * 80)
    print("Running HBase Error Handling Solution Tests")
    print("=" * 80)
    print()
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test cases
    suite.addTests(loader.loadTestsFromTestCase(TestHBaseErrorHandler))
    suite.addTests(loader.loadTestsFromTestCase(TestHBaseConfigValidator))
    suite.addTests(loader.loadTestsFromTestCase(TestIntegration))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print()
    print("=" * 80)
    print("Test Summary")
    print("=" * 80)
    print(f"Tests run: {result.testsRun}")
    print(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print("=" * 80)
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
