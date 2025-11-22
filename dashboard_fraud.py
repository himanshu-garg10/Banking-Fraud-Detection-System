import streamlit as st
import pandas as pd
import requests
import os
import json
import plotly.express as px

API = "http://127.0.0.1:8000"

# ---------------- API CALLER ----------------
def call_api(path: str, timeout=2):
    try:
        r = requests.get(API + path, timeout=timeout)
        if r.status_code == 200:
            return r.json()
        return None
    except:
        return None

# ---------------- PARQUET LOADER ----------------
def load_parquet(folder):
    files = []
    for root, _, fs in os.walk(folder):
        for f in fs:
            if f.endswith(".parquet"):
                files.append(os.path.join(root, f))

    if not files:
        return pd.DataFrame()

    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

# ---------------- ALERT LOADER ----------------
def load_alerts(folder):
    rows = []
    for root, _, fs in os.walk(folder):
        for f in fs:
            fp = os.path.join(root, f)
            try:
                with open(fp, "r") as fh:
                    for line in fh:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            rows.append(json.loads(line))
                        except:
                            rows.append({"raw": line})
            except:
                pass
    return pd.DataFrame(rows)

# ---------------- UI SETTINGS ----------------
st.set_page_config(page_title="Fraud Detection System", layout="wide")

st.sidebar.title("Navigation")
pages = [
    "Dashboard Overview",
    "Live API Metrics",
    "Live Streaming Transactions",
    "Final Fused Data",
    "Aggregates & Trends",
    "Customer Drilldown",
    "High Risk Alerts",
    "Data Explorer",
    "System Status"
]

page = st.sidebar.radio("Go to", pages)

# ---------------- LOAD DATA ----------------
df_final = load_parquet("output/final")
df_agg = load_parquet("output/aggregates")
df_alerts = load_alerts("output/alerts")

# =================================================================
# PAGE 1 — DASHBOARD OVERVIEW
# =================================================================
if page == "Dashboard Overview":
    st.title("Fraud Detection — Real-Time Dashboard")

    c1, c2, c3, c4 = st.columns(4)

    total_tx = len(df_final)
    total_cust = df_final["customer_id"].nunique() if "customer_id" in df_final else 0
    alerts = len(df_alerts)
    max_risk = df_final["risk_score"].max() if "risk_score" in df_final else 0

    c1.metric("Total Transactions", total_tx)
    c2.metric("Customers", total_cust)
    c3.metric("High-Risk Alerts", alerts)
    c4.metric("Max Risk Score", round(max_risk, 2))

    st.markdown("---")
    st.subheader("Risk Level Distribution")

    if "risk_level" in df_final:
        st.plotly_chart(px.histogram(df_final, x="risk_level"), use_container_width=True)
    else:
        st.info("risk_level column not found.")

    st.subheader("Top Risky Customers")
    if "risk_score" in df_final:
        risky = df_final.groupby("customer_id")["risk_score"].max().sort_values(ascending=False).head(10)
        st.bar_chart(risky)
    else:
        st.info("risk_score column missing.")

    # Geo Heatmap disabled (no lat/lon in data)
    st.info("Geo Heatmap disabled (lat/lon not in dataset).")

# =================================================================
# PAGE 2 — LIVE API METRICS
# =================================================================
elif page == "Live API Metrics":
    st.title("Live Backend Metrics (via API)")
    status = call_api("/")
    st.write("Backend:", status)

    st.markdown("---")
    st.subheader("Live Endpoint /latest")
    live_data = call_api("/latest")
    st.json(live_data if live_data else {"error": "API response empty"})

# =================================================================
# PAGE 3 — FINAL FUSED DATA
# =================================================================
elif page == "Final Fused Data":
    st.title("Final Fused Data Output")
    if df_final.empty:
        st.warning("No data found in output/final.")
    else:
        st.dataframe(df_final.head(200))
        st.write("Columns:", df_final.columns.tolist())

# =================================================================
# PAGE 4 — AGGREGATES & TRENDS
# =================================================================
elif page == "Aggregates & Trends":
    st.title("Aggregates & Trends")

    if df_agg.empty:
        st.warning("No aggregated data found.")
    else:
        st.dataframe(df_agg.head())

        for col in ["total_amount", "avg_amt", "total_tx"]:
            if col in df_agg:
                fig = px.line(df_agg, x="customer_id", y=col)
                st.plotly_chart(fig, use_container_width=True)
                break
        else:
            st.info("No numeric trend column found.")

# =================================================================
# PAGE 5 — CUSTOMER DRILLDOWN
# =================================================================
elif page == "Customer Drilldown":
    st.title("Customer Drilldown Analysis")

    if df_final.empty:
        st.warning("No fused data available.")
    else:
        customers = sorted(df_final["customer_id"].unique())
        selected = st.selectbox("Select a Customer", customers)

        view = df_final[df_final["customer_id"] == selected]
        st.dataframe(view)

        if "amount" in view:
            fig = px.line(view, y="amount")
            st.plotly_chart(fig, use_container_width=True)

# =================================================================
# PAGE 6 — HIGH RISK ALERTS
# =================================================================
elif page == "High Risk Alerts":
    st.title("High Risk Alerts")
    if df_alerts.empty:
        st.warning("No alerts found.")
    else:
        st.dataframe(df_alerts.tail(50))

# =================================================================
# PAGE 7 — DATA EXPLORER
# =================================================================
elif page == "Data Explorer":
    st.title("Raw Data Explorer")
    choice = st.selectbox("Dataset", ["Final", "Aggregates", "Alerts"])
    if choice == "Final":
        st.dataframe(df_final)
    elif choice == "Aggregates":
        st.dataframe(df_agg)
    else:
        st.dataframe(df_alerts)

# =================================================================
# PAGE 8 — SYSTEM STATUS
# =================================================================
elif page == "System Status":
    st.title("System Status Overview")

    st.write("Backend API:", API)
    health = call_api("/")
    st.write("Health Check:", health)

    st.subheader("Output folder structure")
    for root, dirs, files in os.walk("output"):
        st.write(root, "->", len(files), "files")

# =================================================================
# PAGE 9 — LIVE STREAMING
# =================================================================
elif page == "Live Streaming Transactions":
    st.title("Live Streaming — Latest Transactions")

    refresh = st.button("Refresh")
    data = call_api("/latest_tx")

    if not data:
        st.warning("No live transactions received from API.")
    else:
        df_live = pd.DataFrame(data)
        st.dataframe(df_live)

        if "amount" in df_live:
            fig = px.line(df_live, y="amount", x=df_live.index)
            st.plotly_chart(fig, use_container_width=True)
