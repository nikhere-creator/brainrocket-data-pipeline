"""Utility functions for the BrainRocket data pipeline."""
import os
import logging
from typing import Dict, Any, Optional
from sqlalchemy import create_engine, text, Engine
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def get_db_connection() -> Engine:
    """Create and return a database connection engine.
    
    Returns:
        Engine: SQLAlchemy database engine
        
    Raises:
        ValueError: If database connection parameters are missing
    """
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'gaming_db')
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', 'postgres')
    
    if not all([db_host, db_port, db_name, db_user, db_password]):
        raise ValueError("Missing database connection parameters")
    
    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    try:
        engine = create_engine(connection_string)
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connection established successfully")
        return engine
    except SQLAlchemyError as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


def execute_sql_file(engine: Engine, file_path: str) -> None:
    """Execute SQL commands from a file.
    
    Args:
        engine: Database engine
        file_path: Path to SQL file
        
    Raises:
        FileNotFoundError: If SQL file doesn't exist
        SQLAlchemyError: If SQL execution fails
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"SQL file not found: {file_path}")
    
    try:
        with open(file_path, 'r') as file:
            sql_commands = file.read()
        
        with engine.connect() as conn:
            # Split commands by semicolon and execute each
            for command in sql_commands.split(';'):
                command = command.strip()
                if command:
                    conn.execute(text(command))
            conn.commit()
        
        logger.info(f"Successfully executed SQL file: {file_path}")
    except SQLAlchemyError as e:
        logger.error(f"Failed to execute SQL file {file_path}: {e}")
        raise


def refresh_materialized_view(engine: Engine, view_name: str = 'mv_daily_game_metrics') -> None:
    """Refresh a materialized view.
    
    Args:
        engine: Database engine
        view_name: Name of the materialized view to refresh
    """
    try:
        with engine.connect() as conn:
            conn.execute(text(f"REFRESH MATERIALIZED VIEW {view_name}"))
            conn.commit()
        logger.info(f"Successfully refreshed materialized view: {view_name}")
    except SQLAlchemyError as e:
        logger.error(f"Failed to refresh materialized view {view_name}: {e}")
        raise


def validate_transaction_data(df: pd.DataFrame) -> pd.DataFrame:
    """Validate and clean transaction data.
    
    Args:
        df: Input DataFrame with transaction data
        
    Returns:
        pd.DataFrame: Cleaned and validated DataFrame
    """
    # Make a copy to avoid modifying the original
    df_clean = df.copy()
    
    # Validate required columns
    required_columns = ['game_id', 'location_id', 'user_id', 'transaction_type', 'amount', 'transaction_date']
    missing_columns = [col for col in required_columns if col not in df_clean.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Clean data
    df_clean['transaction_type'] = df_clean['transaction_type'].str.lower().str.strip()
    df_clean['user_id'] = df_clean['user_id'].astype(str).str.strip()
    df_clean['amount'] = pd.to_numeric(df_clean['amount'], errors='coerce')
    df_clean['transaction_date'] = pd.to_datetime(df_clean['transaction_date'], errors='coerce')
    
    # Filter out invalid rows
    initial_count = len(df_clean)
    df_clean = df_clean.dropna(subset=['game_id', 'location_id', 'user_id', 'transaction_type', 'amount', 'transaction_date'])
    df_clean = df_clean[df_clean['transaction_type'].isin(['purchase', 'in-game', 'subscription'])]
    df_clean = df_clean[df_clean['amount'] > 0]
    
    removed_count = initial_count - len(df_clean)
    if removed_count > 0:
        logger.warning(f"Removed {removed_count} invalid rows during validation")
    
    return df_clean


def load_data_to_postgres(engine: Engine, df: pd.DataFrame, table_name: str, if_exists: str = 'append') -> int:
    """Load DataFrame data into PostgreSQL table.
    
    Args:
        engine: Database engine
        df: DataFrame to load
        table_name: Target table name
        if_exists: What to do if table exists ('fail', 'replace', 'append')
        
    Returns:
        int: Number of rows inserted
    """
    try:
        rows_inserted = df.to_sql(
            table_name, 
            engine, 
            if_exists=if_exists, 
            index=False,
            method='multi'
        )
        logger.info(f"Successfully loaded {rows_inserted} rows into {table_name}")
        return rows_inserted
    except Exception as e:
        logger.error(f"Failed to load data into {table_name}: {e}")
        raise
