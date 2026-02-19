import logging
import os
from pipeline.extract import extract_from_api, save_raw_data
from pipeline.transform import transform_user_data
from pipeline.load import load_to_sqlite

# Configuration
API_URL = "https://jsonplaceholder.typicode.com/users"
RAW_DATA_PATH = "data/raw_users.json"
DB_PATH = "data/warehouse.db"
TABLE_NAME = "refined_users"

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ETL_Orchestrator")

def run_pipeline():
    """Main ETL Orchestration function."""
    logger.info("--- Starting ETL Pipeline v1.0 ---")
    
    # 0. Preparation
    if not os.path.exists("data"):
        os.makedirs("data")

    # 1. Extraction
    raw_data = extract_from_api(API_URL)
    if not raw_data:
        logger.error("Stop: No data extracted.")
        return
    save_raw_data(raw_data, RAW_DATA_PATH)

    # 2. Transformation
    clean_df = transform_user_data(raw_data)
    if clean_df.empty:
        logger.error("Stop: Transformation produced an empty dataset.")
        return

    # 3. Loading
    load_to_sqlite(clean_df, DB_PATH, TABLE_NAME)

    logger.info("--- ETL Pipeline Finished Successfully ---")

if __name__ == "__main__":
    run_pipeline()
