import streamlit as st
import pandas as pd
import sqlite3
import os
import time
from main import run_pipeline, DB_PATH, TABLE_NAME

# 1. Page Configuration
st.set_page_config(page_title="ETL Pipeline Monitor", page_icon="⚙️", layout="wide")

# 2. UI Header
st.title("⚙️ Production-Ready ETL Pipeline Monitor")
st.markdown("Automating data ingestion, transformation, and warehousing in real-time.")

# Sidebar Controls
with st.sidebar:
    st.header("🎮 Pipeline Controls")
    
    # Live Stream Toggle
    if 'is_streaming' not in st.session_state:
        st.session_state.is_streaming = False
        
    streaming_toggle = st.toggle("🛰️ Live Stream Mode", value=st.session_state.is_streaming)
    st.session_state.is_streaming = streaming_toggle
    
    st.divider()
    
    if not st.session_state.is_streaming:
        run_btn = st.button("🚀 Trigger ETL Pipeline", type="primary", use_container_width=True)
    else:
        st.warning("Streaming active: Pipeline will run every 10s.")
        run_btn = False
    
    st.divider()
    st.info("Source: JSONPlaceholder API")
    st.info("Target: SQLite Warehouse")

# 3. Main Logic
def execute_etl():
    with st.status("Pipeline Executing...", expanded=False) as status:
        st.write("🔍 Extracting raw data from API...")
        try:
            run_pipeline()
            st.write("✨ Transformation applied.")
            st.write("💾 Loading to Warehouse...")
            status.update(label=f"✅ Last Run: {time.strftime('%H:%M:%S')}", state="complete")
            return True
        except Exception as e:
            st.error(f"Pipeline Failed: {e}")
            return False

if run_btn or st.session_state.is_streaming:
    execute_etl()
    if st.session_state.is_streaming:
        time.sleep(10)
        st.rerun()


# 4. Data Visualization
if os.path.exists(DB_PATH):
    conn = sqlite3.connect(DB_PATH)
    
    st.subheader("📊 Warehouse Insights")
    
    # Load data for visualization
    df = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME}", conn)
    
    if not df.empty:
        # KPI Row
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Records", len(df))
        c2.metric("Unique Cities", df['city'].nunique())
        c3.metric("Companies Tracked", df['company'].nunique())
        
        st.divider()
        
        # Table View
        st.write("### 🗄️ Refined Data Preview")
        st.dataframe(df, use_container_width=True)
        
        # Simple Visualization
        st.write("### 📍 Geographic Distribution")
        city_counts = df['city'].value_counts().reset_index()
        city_counts.columns = ['City', 'Count']
        st.bar_chart(city_counts.set_index('City'))
        
    conn.close()
else:
    st.info("The Data Warehouse is currently empty. Click 'Trigger ETL Pipeline' to populate it.")
    st.image("https://images.unsplash.com/photo-1558494949-ef010cbdcc51?auto=format&fit=crop&q=80&w=1200", caption="Modular Data Infrastructure")
