# ==================================================================================
# CATALOG MANAGER
# ==================================================================================
# Manages Iceberg catalog creation and database setup.
# ==================================================================================

import sys
import os
from typing import Any
from pyflink.table import TableEnvironment


class CatalogManager:
    """Manages Iceberg catalog and database operations."""
    
    def __init__(self, table_env: TableEnvironment, iceberg_config: dict):
        """Initialize catalog manager.
        
        Args:
            table_env: Flink TableEnvironment
            iceberg_config: Iceberg configuration dictionary
        """
        self.table_env = table_env
        self.iceberg_config = iceberg_config
        self.catalog_name = "s3_tables"
    
    def create_catalog(self):
        """Create and configure Iceberg catalog."""
        print("  Creating Iceberg catalog...")
        
        warehouse = self.iceberg_config.get("warehouse")
        region = self.iceberg_config.get("region", "ap-south-1")
        namespace = self.iceberg_config.get("namespace", "analytics")
        
        print(f"    Warehouse: {warehouse}")
        print(f"    Region: {region}")
        print(f"    Namespace: {namespace}")
        
        # Step 1: Create Catalog
        try:
            is_local = os.getenv("FLINK_ENV") == "local"
            
            if is_local:
                # LOCAL: Use REST Catalog (Tabular) pointing to S3 Tables
                print("    Target: Local Development (Console Output)")
                print("    Note: In local mode, data is printed to console instead of written to S3 Tables")
                print("    For full S3 Tables testing, use AWS Managed Flink")
                print("    ✓ Local mode: Skipping Iceberg catalog setup")
                return  # Skip catalog creation in local mode
            else:
                # PRODUCTION: Use S3 Tables Catalog (AWS Managed)
                print("    Target: AWS S3 Tables (Production)")
                
                # Validate S3 Tables ARN format
                if not warehouse.startswith("arn:aws:s3tables:"):
                    raise ValueError(
                        f"Invalid S3 Tables warehouse format: {warehouse}\n"
                        f"Expected format: arn:aws:s3tables:<region>:<account-id>:bucket/<bucket-name>\n"
                        f"Example: arn:aws:s3tables:ap-south-1:123456789012:bucket/my-s3tables-bucket"
                    )
                
                create_catalog_sql = f"""
                    CREATE CATALOG {self.catalog_name} WITH (
                        'type' = 'iceberg',
                        'catalog-impl' = 'software.amazon.s3tables.iceberg.S3TablesCatalog',
                        'warehouse' = '{warehouse}',
                        'region' = '{region}'
                    )
                """
            
            self.table_env.execute_sql(create_catalog_sql)
            print(f"    ✓ Catalog '{self.catalog_name}' created successfully")
            
        except Exception as e:
            error_msg = str(e).lower()
            if "already exists" in error_msg or f"catalog {self.catalog_name} exists" in error_msg:
                print(f"    ✓ Catalog '{self.catalog_name}' already exists")
            else:
                print(f"    Warning during catalog creation: {e}")
                # Continue anyway - catalog might exist
        
        # Step 2: Use Catalog
        try:
            self.table_env.use_catalog(self.catalog_name)
            print(f"    ✓ Switched to catalog '{self.catalog_name}'")
        except Exception as e:
            print(f"    ✗ FATAL: Cannot use catalog {self.catalog_name}: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc(file=sys.stderr)
            sys.exit(1)
        
        # Step 3: Create Database (without IF NOT EXISTS)
        try:
            create_db_sql = f"CREATE DATABASE IF NOT EXISTS {namespace}"
            self.table_env.execute_sql(create_db_sql)
            print(f"    ✓ Database '{namespace}' verified/created")
        except Exception as e:
            error_msg = str(e).lower()
            if "already exists" in error_msg or f"database {namespace} exists" in error_msg:
                print(f"    ✓ Database '{namespace}' already exists")
            else:
                print(f"    Warning during database creation: {e}")
                # Continue anyway - database might exist
        
        # Step 4: Use Database
        try:
            self.table_env.use_database(namespace)
            print(f"    ✓ Switched to database '{namespace}'")
            print(f"  ✓ Catalog and Database ready")
        except Exception as e:
            print(f"    ✗ FATAL: Cannot use database {namespace}: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc(file=sys.stderr)
            sys.exit(1)
    
    def get_catalog_name(self) -> str:
        """Get the name of the Iceberg catalog.
        
        Returns:
            Catalog name
        """
        return self.catalog_name
    
    def get_namespace(self) -> str:
        """Get the namespace (database) name.
        
        Returns:
            Namespace name
        """
        return self.iceberg_config.get('namespace', 'analytics')