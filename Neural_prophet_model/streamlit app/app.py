import streamlit as st
import pandas as pd
import os

# Set page configuration
st.set_page_config(page_title="Crypto Forecast Dashboard", layout="centered")

# Styled title and subtitle for dark mode (white text)
st.markdown("""
    <div style='text-align: center; padding-top: 10px; padding-bottom: 0px;'>
        <h1 style='font-size: 3em; color: #FFFFFF;'>📊 Crypto Forecast Dashboard</h1>
        <p style='font-size: 1.2em; color: #BBBBBB;'>Your guide to trade with confidence — powered by AI</p>
    </div>
""", unsafe_allow_html=True)

# Folder containing forecast CSVs
folder_path = "forecasts"
csv_files = [f for f in os.listdir(folder_path) if f.startswith("forecast_") and f.endswith(".csv")]
coin_map = {f.replace("forecast_", "").replace("_12months.csv", ""): f for f in csv_files}

if not coin_map:
    st.warning("⚠️ No forecast CSV files found in the 'forecasts' folder.")
else:
    selected_coin = st.selectbox("Choose a cryptocurrency:", sorted(coin_map.keys()))

    if selected_coin:
        selected_file = coin_map[selected_coin]
        df_raw = pd.read_csv(os.path.join(folder_path, selected_file))

        # Filter and rename important columns only
        if "trend" in df_raw.columns:
            df = df_raw[["ds", "yhat1", "trend"]].rename(columns={
                "ds": "Date",
                "yhat1": "Predicted Price",
                "trend": "Trend"
            })
        else:
            df = df_raw[["ds", "yhat1"]].rename(columns={
                "ds": "Date",
                "yhat1": "Predicted Price"
            })

        # Display forecast chart
        st.subheader(f"{selected_coin.capitalize()} Price Forecast")
        st.line_chart(df.set_index("Date")[["Predicted Price"]])

        # Show data table
        if st.checkbox("Show in table"):
            st.subheader("📋 Forecast Table")
            st.dataframe(
                df.reset_index(drop=True).style
                    .format({"Predicted Price": "{:.2f}", "Trend": "{:.2f}" if "Trend" in df.columns else None})
                    .highlight_max(axis=0),
                use_container_width=True
            )
