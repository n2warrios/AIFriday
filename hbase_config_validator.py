"""
HBase Configuration Validator
Validates HBase setup and provides configuration recommendations for Spark SQL integration.

This script checks:
- Required JAR files
- Configuration files
- Service availability
- Classpath settings
"""

import os
import sys
import logging
from pathlib import Path
from typing import List, Dict, Tuple

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HBaseConfigValidator:
    """Validates HBase configuration for Spark SQL integration."""
    
    # Required JAR files for HBase-Spark integration
    REQUIRED_JARS = [
        "hbase-client",
        "hbase-common",
        "hbase-protocol",
        "hbase-server",
        "hive-hbase-handler",
        "hbase-hadoop-compat"
    ]
    
    # Common HBase configuration file locations
    CONFIG_PATHS = [
        "/etc/hbase/conf",
        "/opt/hbase/conf",
        "/usr/local/hbase/conf",
        "$HBASE_HOME/conf",
        "./conf"
    ]
    
    def __init__(self, hbase_home: str = None):
        """
        Initialize validator.
        
        Args:
            hbase_home: Optional HBASE_HOME path override
        """
        self.hbase_home = hbase_home or os.environ.get("HBASE_HOME", "")
        self.issues = []
        self.warnings = []
        self.recommendations = []
        
    def validate_all(self) -> Dict[str, any]:
        """
        Run all validation checks.
        
        Returns:
            Dictionary with validation results
        """
        logger.info("Starting HBase configuration validation...")
        
        results = {
            'environment': self._check_environment(),
            'jars': self._check_jars(),
            'config_files': self._check_config_files(),
            'classpath': self._check_classpath(),
            'issues': self.issues,
            'warnings': self.warnings,
            'recommendations': self.recommendations
        }
        
        return results
    
    def _check_environment(self) -> Dict[str, str]:
        """Check environment variables."""
        logger.info("Checking environment variables...")
        
        env_vars = {
            'HBASE_HOME': os.environ.get('HBASE_HOME', ''),
            'HADOOP_HOME': os.environ.get('HADOOP_HOME', ''),
            'SPARK_HOME': os.environ.get('SPARK_HOME', ''),
            'JAVA_HOME': os.environ.get('JAVA_HOME', ''),
            'HADOOP_CLASSPATH': os.environ.get('HADOOP_CLASSPATH', ''),
        }
        
        for var, value in env_vars.items():
            if value:
                logger.info(f"  ✓ {var} = {value}")
            else:
                logger.warning(f"  ✗ {var} not set")
                self.warnings.append(f"{var} environment variable not set")
        
        if not env_vars['HBASE_HOME']:
            self.recommendations.append(
                "Set HBASE_HOME environment variable: export HBASE_HOME=/path/to/hbase"
            )
        
        return env_vars
    
    def _check_jars(self) -> Dict[str, List[str]]:
        """Check for required JAR files."""
        logger.info("Checking for required JAR files...")
        
        found_jars = []
        missing_jars = []
        
        # Search locations
        search_paths = []
        if self.hbase_home:
            search_paths.append(os.path.join(self.hbase_home, "lib"))
        
        hadoop_home = os.environ.get('HADOOP_HOME')
        if hadoop_home:
            search_paths.append(os.path.join(hadoop_home, "share", "hadoop", "common", "lib"))
        
        spark_home = os.environ.get('SPARK_HOME')
        if spark_home:
            search_paths.append(os.path.join(spark_home, "jars"))
        
        # Check each required JAR
        for jar_name in self.REQUIRED_JARS:
            found = False
            
            for search_path in search_paths:
                if os.path.exists(search_path):
                    for file in os.listdir(search_path):
                        if jar_name in file and file.endswith('.jar'):
                            found_jars.append(os.path.join(search_path, file))
                            found = True
                            logger.info(f"  ✓ Found: {file}")
                            break
                if found:
                    break
            
            if not found:
                missing_jars.append(jar_name)
                logger.warning(f"  ✗ Missing: {jar_name}*.jar")
                self.issues.append(f"Missing JAR: {jar_name}")
        
        if missing_jars:
            self.recommendations.append(
                f"Download missing JARs: {', '.join(missing_jars)}"
            )
            self.recommendations.append(
                "Add JARs to Spark with: --jars /path/to/jar1.jar,/path/to/jar2.jar"
            )
        
        return {
            'found': found_jars,
            'missing': missing_jars
        }
    
    def _check_config_files(self) -> Dict[str, bool]:
        """Check for required configuration files."""
        logger.info("Checking configuration files...")
        
        config_files = {
            'hbase-site.xml': False,
            'core-site.xml': False,
            'hdfs-site.xml': False,
            'hive-site.xml': False
        }
        
        # Expand config paths
        search_paths = []
        for path_template in self.CONFIG_PATHS:
            path = os.path.expandvars(path_template)
            if os.path.exists(path):
                search_paths.append(path)
        
        # Check each config file
        for config_file in config_files.keys():
            for search_path in search_paths:
                config_path = os.path.join(search_path, config_file)
                if os.path.exists(config_path):
                    config_files[config_file] = True
                    logger.info(f"  ✓ Found: {config_path}")
                    break
            
            if not config_files[config_file]:
                logger.warning(f"  ✗ Not found: {config_file}")
                if config_file == 'hbase-site.xml':
                    self.issues.append(f"Missing critical config: {config_file}")
                else:
                    self.warnings.append(f"Config file not found: {config_file}")
        
        if not config_files['hbase-site.xml']:
            self.recommendations.append(
                "Create or locate hbase-site.xml with HBase cluster configuration"
            )
            self.recommendations.append(
                "Copy to Spark conf: cp $HBASE_HOME/conf/hbase-site.xml $SPARK_HOME/conf/"
            )
        
        return config_files
    
    def _check_classpath(self) -> Dict[str, any]:
        """Check if HBase is in classpath."""
        logger.info("Checking classpath configuration...")
        
        classpath_info = {
            'hadoop_classpath_set': False,
            'contains_hbase': False
        }
        
        hadoop_classpath = os.environ.get('HADOOP_CLASSPATH', '')
        
        if hadoop_classpath:
            classpath_info['hadoop_classpath_set'] = True
            if 'hbase' in hadoop_classpath.lower():
                classpath_info['contains_hbase'] = True
                logger.info("  ✓ HBase found in HADOOP_CLASSPATH")
            else:
                logger.warning("  ✗ HBase not in HADOOP_CLASSPATH")
                self.warnings.append("HBase not in HADOOP_CLASSPATH")
        else:
            logger.warning("  ✗ HADOOP_CLASSPATH not set")
            self.warnings.append("HADOOP_CLASSPATH not set")
        
        if not classpath_info['contains_hbase']:
            self.recommendations.append(
                "Add HBase to classpath: export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$HBASE_HOME/lib/*"
            )
        
        return classpath_info
    
    def generate_spark_submit_command(self, script_path: str, 
                                      jar_paths: List[str] = None) -> str:
        """
        Generate a spark-submit command with proper HBase configuration.
        
        Args:
            script_path: Path to the Python script to execute
            jar_paths: List of JAR file paths to include
            
        Returns:
            Complete spark-submit command
        """
        if jar_paths is None:
            jar_paths = []
        
        # Build command
        cmd_parts = [
            "spark-submit",
            "--master yarn",  # or local[*] for local mode
            "--deploy-mode client"
        ]
        
        # Add JARs
        if jar_paths:
            cmd_parts.append(f"--jars {','.join(jar_paths)}")
        
        # Add configuration
        cmd_parts.extend([
            "--conf spark.sql.hive.metastore.version=3.1.2",
            "--conf spark.sql.hive.metastore.jars=path",
            "--conf spark.executor.memory=4g",
            "--conf spark.driver.memory=2g"
        ])
        
        # Add script
        cmd_parts.append(script_path)
        
        return " \\\n  ".join(cmd_parts)
    
    def print_report(self, results: Dict) -> None:
        """Print validation report."""
        print("\n" + "=" * 80)
        print("HBase Configuration Validation Report")
        print("=" * 80)
        
        print("\n📋 ENVIRONMENT VARIABLES:")
        for var, value in results['environment'].items():
            status = "✓" if value else "✗"
            print(f"  {status} {var}: {value or 'NOT SET'}")
        
        print("\n📦 JAR FILES:")
        print(f"  Found: {len(results['jars']['found'])} JAR(s)")
        if results['jars']['missing']:
            print(f"  Missing: {', '.join(results['jars']['missing'])}")
        
        print("\n⚙️  CONFIGURATION FILES:")
        for config, found in results['config_files'].items():
            status = "✓" if found else "✗"
            print(f"  {status} {config}")
        
        print("\n🔍 CLASSPATH:")
        classpath = results['classpath']
        print(f"  HADOOP_CLASSPATH set: {'✓' if classpath['hadoop_classpath_set'] else '✗'}")
        print(f"  Contains HBase: {'✓' if classpath['contains_hbase'] else '✗'}")
        
        # Issues and warnings
        if results['issues']:
            print("\n❌ CRITICAL ISSUES:")
            for issue in results['issues']:
                print(f"  • {issue}")
        
        if results['warnings']:
            print("\n⚠️  WARNINGS:")
            for warning in results['warnings']:
                print(f"  • {warning}")
        
        # Recommendations
        if results['recommendations']:
            print("\n💡 RECOMMENDATIONS:")
            for i, rec in enumerate(results['recommendations'], 1):
                print(f"  {i}. {rec}")
        
        # Overall status
        print("\n" + "=" * 80)
        if not results['issues']:
            print("✅ Validation passed! Configuration looks good.")
        else:
            print("❌ Validation failed! Please address the issues above.")
        print("=" * 80 + "\n")


def main():
    """Main entry point for the validator."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Validate HBase configuration for Spark SQL integration"
    )
    parser.add_argument(
        '--hbase-home',
        help='Path to HBASE_HOME directory',
        default=None
    )
    parser.add_argument(
        '--generate-command',
        help='Generate spark-submit command for a script',
        metavar='SCRIPT_PATH'
    )
    parser.add_argument(
        '--jars',
        help='Comma-separated list of JAR files to include',
        default=''
    )
    
    args = parser.parse_args()
    
    # Run validation
    validator = HBaseConfigValidator(hbase_home=args.hbase_home)
    results = validator.validate_all()
    validator.print_report(results)
    
    # Generate spark-submit command if requested
    if args.generate_command:
        jar_list = [j.strip() for j in args.jars.split(',') if j.strip()]
        if results['jars']['found']:
            jar_list.extend(results['jars']['found'])
        
        print("\n📝 GENERATED SPARK-SUBMIT COMMAND:")
        print("-" * 80)
        cmd = validator.generate_spark_submit_command(
            args.generate_command,
            jar_list
        )
        print(cmd)
        print("-" * 80)
    
    # Exit with error code if critical issues found
    sys.exit(1 if results['issues'] else 0)


if __name__ == "__main__":
    main()
