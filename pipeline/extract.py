import requests
import json
import logging

logger = logging.getLogger(__name__)

def extract_from_api(url: str):
    """Extracts data from a public API (e.g., Placeholder Users)."""
    logger.info(f"Extracting data from {url}...")
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Successfully extracted {len(data)} records.")
        return data
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        return []

def save_raw_data(data: list, filepath: str):
    """Saves the extracted raw JSON to a local file (Data Lake simulation)."""
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=4)
    logger.info(f"Raw data cached at {filepath}")
