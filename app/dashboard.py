import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import os

@st.cache_data
def load_data():
    csv_path = os.path.join(os.path.dirname(__file__), "../data/SP500_Breach_Report_Latest.csv")
    csv_path = os.path.abspath(csv_path)

    if not os.path.exists(csv_path):
        st.warning("⚠️ No data file found. Please add SP500_Breach_Report_Latest.csv to the data folder.")
        return pd.DataFrame()

    df = pd.read_csv(csv_path)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    return df


df = load_data()

st.sidebar.header("📊 Filters")
categories = df["Breach Category"].dropna().unique().tolist()
default_cats = [c for c in ["WA - 3σ", "WA - (-3σ)"] if c in categories] or categories
selected = st.sidebar.multiselect("Select Breach Category", categories, default=default_cats)
filtered = df[df["Breach Category"].isin(selected)]

col1, col2, col3 = st.columns(3)
col1.metric("Total Companies", len(filtered))
col2.metric("Avg Weighted Gap", f"{filtered['Weighted Avg Gap'].mean():.4f}")
col3.metric("Last Updated", datetime.now().strftime("%Y-%m-%d %H:%M"))

st.markdown("### Breach Table")
st.dataframe(filtered, use_container_width=True)

st.markdown("### Weighted Gap vs 5-Day Return")
fig = px.scatter(filtered, x="Weighted Avg Gap", y="+5d Return",
                 color="Breach Category", hover_data=["Ticker"])
st.plotly_chart(fig, use_container_width=True)
