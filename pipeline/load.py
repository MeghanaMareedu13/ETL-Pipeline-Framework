from sqlalchemy import create_engine
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def load_to_sqlite(df: pd.DataFrame, db_path: str, table_name: str):
    """Loads a DataFrame into a SQLite database."""
    if df.empty:
        logger.warning("No data to load.")
        return

    logger.info(f"Loading data into {db_path} | Table: {table_name}...")
    try:
        engine = create_engine(f'sqlite:///{db_path}')
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logger.info("Load phase complete.")
    except Exception as e:
        logger.error(f"Load failed: {e}")
