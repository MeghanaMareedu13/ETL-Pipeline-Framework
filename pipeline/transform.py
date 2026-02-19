import pandas as pd
import logging

logger = logging.getLogger(__name__)

def transform_user_data(raw_data: list):
    """Transforms raw user JSON into a clean DataFrame."""
    logger.info("Starting transformation...")
    
    if not raw_data:
        return pd.DataFrame()

    # Convert to DataFrame
    df = pd.json_normalize(raw_data)
    
    # 1. Column Renaming & Selection
    df = df[['id', 'name', 'username', 'email', 'address.city', 'company.name']]
    df.columns = ['user_id', 'full_name', 'username', 'email', 'city', 'company']
    
    # 2. Data Cleaning
    df['email'] = df['email'].str.lower()
    
    # 3. Enrichment: Split Name
    df[['first_name', 'last_name']] = df['full_name'].str.split(' ', n=1, expand=True)
    
    # 4. Filter: Keep only those with specific companies (simulating business logic)
    # df = df[df['company'].str.contains('Considine', na=False)] 
    
    logger.info(f"Transformation complete. Resulting shape: {df.shape}")
    return df
