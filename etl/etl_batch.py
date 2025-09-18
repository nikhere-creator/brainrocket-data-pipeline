"""Batch ETL process for gaming transaction data."""
import argparse
import logging
import pandas as pd
from typing import Optional
from etl.utils import (
    get_db_connection, 
    validate_transaction_data, 
    load_data_to_postgres,
    refresh_materialized_view,
    logger
)


def run_etl_pipeline(input_file: str, truncate: bool = False) -> None:
    """Run the complete ETL pipeline.
    
    Args:
        input_file: Path to input CSV file
        truncate: Whether to truncate the table before loading
    """
    try:
        logger.info(f"Starting ETL pipeline for {input_file}")
        
        # Step 1: Get database connection
        engine = get_db_connection()
        
        # Step 2: Extract - Read CSV file
        logger.info(f"Reading data from {input_file}")
        df = pd.read_csv(input_file)
        logger.info(f"Read {len(df)} rows from {input_file}")
        
        # Step 3: Transform - Validate and clean data
        logger.info("Validating and cleaning transaction data")
        df_clean = validate_transaction_data(df)
        logger.info(f"After validation: {len(df_clean)} valid rows")
        
        # Step 4: Load - Load data to PostgreSQL
        logger.info("Loading data to PostgreSQL")
        if_exists = 'replace' if truncate else 'append'
        rows_inserted = load_data_to_postgres(engine, df_clean, 'fact_transactions', if_exists)
        
        # Step 5: Refresh materialized view
        logger.info("Refreshing materialized view")
        refresh_materialized_view(engine)
        
        # Step 6: Generate summary report
        generate_summary_report(df_clean, rows_inserted)
        
        logger.info("ETL pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        raise


def generate_summary_report(df: pd.DataFrame, rows_inserted: int) -> None:
    """Generate and log a summary report of the ETL process.
    
    Args:
        df: Cleaned DataFrame
        rows_inserted: Number of rows inserted
    """
    logger.info("=== ETL SUMMARY REPORT ===")
    logger.info(f"Rows processed: {rows_inserted}")
    logger.info(f"Total revenue: ${df['amount'].sum():.2f}")
    logger.info(f"Average transaction: ${df['amount'].mean():.2f}")
    
    # Transaction type distribution
    type_counts = df['transaction_type'].value_counts()
    for ttype, count in type_counts.items():
        percentage = (count / len(df)) * 100
        logger.info(f"  {ttype}: {count} ({percentage:.1f}%)")
    
    # Platform distribution
    if 'platform' in df.columns:
        platform_counts = df['platform'].value_counts()
        logger.info("Platform distribution:")
        for platform, count in platform_counts.items():
            percentage = (count / len(df)) * 100
            logger.info(f"  {platform}: {count} ({percentage:.1f}%)")
    
    logger.info("==========================")


def initialize_database() -> None:
    """Initialize the database with schema and seed data."""
    try:
        logger.info("Initializing database...")
        engine = get_db_connection()
        
        # Execute schema SQL
        from etl.utils import execute_sql_file
        execute_sql_file(engine, 'sql/schema.sql')
        execute_sql_file(engine, 'sql/seed_data.sql')
        
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise


def main():
    """Main function to run the ETL pipeline."""
    parser = argparse.ArgumentParser(description='Run ETL pipeline for gaming transaction data')
    parser.add_argument('--input', type=str, required=True,
                       help='Input CSV file path')
    parser.add_argument('--truncate', action='store_true',
                       help='Truncate table before loading (default: append)')
    parser.add_argument('--init-db', action='store_true',
                       help='Initialize database schema and seed data')
    
    args = parser.parse_args()
    
    try:
        # Initialize database if requested
        if args.init_db:
            initialize_database()
        
        # Run ETL pipeline
        run_etl_pipeline(args.input, args.truncate)
        
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        exit(1)


if __name__ == '__main__':
    main()
