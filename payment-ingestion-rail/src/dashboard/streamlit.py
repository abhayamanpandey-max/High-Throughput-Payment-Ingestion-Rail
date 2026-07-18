import streamlit as st
import pandas as pd
import time
from databricks import sql

# Page configuration
st.set_page_config(page_title="Real-Time Fraud Dashboard", page_icon="💳", layout="wide")
st.title("💳 High-Throughput Real-Time Payment Anomaly Monitor")

# Connection helper for Databricks SQL Warehouse (Gold Layer)
def load_gold_metrics():
    # In production, use Streamlit Secrets to manage credentials securely
    try:
        connection = sql.connect(
            server_hostname=st.secrets["DB_HOSTNAME"],
            http_path=st.secrets["DB_HTTP_PATH"],
            access_token=st.secrets["DB_TOKEN"]
        )
        cursor = connection.cursor()
        cursor.execute("SELECT window_start, window_end, account_id, tx_count_5m, total_spent_5m, is_anomaly FROM hive_metastore.default.payment_gold ORDER BY window_end DESC LIMIT 50")
        result = cursor.fetchall()
        
        # Convert to DataFrame
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(result, columns=columns)
        cursor.close()
        connection.close()
        return df
    except Exception as e:
        # Fallback dummy data for local testing if Databricks is offline
        return pd.DataFrame({
            'window_end': [pd.Timestamp.now()] * 3,
            'account_id': ['acc_9831', 'acc_1245', 'acc_0982'],
            'tx_count_5m': [10, 2, 15],
            'total_spent_5m': [12450.00, 45.50, 21000.00],
            'is_anomaly': [True, False, True]
        })

# Auto-refresh loop to simulate streaming dashboard
placeholder = st.empty()

while True:
    df = load_gold_metrics()
    anomalies = df[df['is_anomaly'] == True]
    
    with placeholder.container():
        # High-level metric boxes
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Monitored Accounts (Active)", len(df['account_id'].unique()))
        col2.metric("Total Windows Scanned", len(df))
        col3.metric("🔴 Active Anomalies Detected", len(anomalies), delta_color="inverse")
        
        # Split layout for charts and raw tables
        left_chart, right_table = st.columns([1, 1])
        
        with left_chart:
            st.subheader("⚠️ High-Risk Activity Accounts (Velocity Check)")
            if not anomalies.empty:
                st.bar_chart(data=anomalies, x='account_id', y='total_spent_5m')
            else:
                st.info("No active fraud anomalies flagged within the rolling window.")
                
        with right_table:
            st.subheader("📋 Raw Gold Aggregation Records")
            st.dataframe(df, use_container_width=True)
            
    time.sleep(5)  # Refresh metrics dashboard every 5 seconds
