import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import io
import boto3
from botocore.exceptions import ClientError

# Streamlit page configuration
st.set_page_config(page_title="StockIQ", layout="wide", page_icon="📦")

# Initialize session state
if "access_token" not in st.session_state:
    st.session_state.access_token = None

# Login page
if not st.session_state.access_token:
    st.title("StockIQ: Login")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    if st.button("Login"):
        try:
            response = requests.post("http://localhost:8000/auth/token", data={"username": username, "password": password})
            if response.status_code == 200:
                st.session_state.access_token = response.json()["access_token"]
                st.success("Logged in successfully!")
                st.experimental_rerun()
            else:
                st.error(f"Login failed: {response.json().get('detail', 'Unknown error')}")
        except Exception as e:
            st.error(f"Error logging in: {str(e)}")
else:
    # Main app
    st.title("StockIQ: AI-Powered Supply Chain Optimization")
    st.markdown("Upload sales data to predict demand and optimize inventory.")
    if st.button("Logout"):
        st.session_state.access_token = None
        st.experimental_rerun()

    # Headers for authenticated requests
    headers = {"Authorization": f"Bearer {st.session_state.access_token}"}

    # File upload section
    st.header("Upload Sales Data")
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

    if uploaded_file is not None:
        # Read the uploaded file
        df = pd.read_csv(uploaded_file)
        
        # Validate columns
        required_columns = ["date", "product_id", "quantity"]
        if all(col in df.columns for col in required_columns):
            st.success("File validated successfully!")
            
            # Display uploaded data
            st.subheader("Uploaded Sales Data")
            st.dataframe(df.head(), use_container_width=True)
            
            # Plot sales data
            st.subheader("Sales Trend")
            df["date"] = pd.to_datetime(df["date"])
            fig = px.line(df, x="date", y="quantity", color="product_id", title="Sales Quantity Over Time")
            st.plotly_chart(fig, use_container_width=True)
            
            # Upload to FastAPI backend
            if st.button("Upload to Server"):
                try:
                    # Reset file pointer to start
                    uploaded_file.seek(0)
                    # Send file to FastAPI endpoint
                    files = {"file": (uploaded_file.name, uploaded_file, "text/csv")}
                    response = requests.post("http://localhost:8000/data/upload", files=files, headers=headers)
                    
                    if response.status_code == 200:
                        message = response.json().get("message", "File uploaded successfully")
                        s3_filename = response.json().get("s3_filename", "Unknown")
                        st.success(f"{message} (S3 path: {s3_filename})")
                        # Store filename in session state
                        if "uploaded_files" not in st.session_state:
                            st.session_state.uploaded_files = []
                        if s3_filename not in st.session_state.uploaded_files:
                            st.session_state.uploaded_files.append(s3_filename)
                    else:
                        st.error(f"Upload failed: {response.json().get('detail', 'Unknown error')}")
                except Exception as e:
                    st.error(f"Error connecting to server: {str(e)}")
        else:
            st.error(f"CSV must contain columns: {', '.join(required_columns)}")

    # Retrieve data from S3
    st.header("Retrieve Sales Data from S3")
    if "uploaded_files" in st.session_state and st.session_state.uploaded_files:
        selected_filename = st.selectbox("Select a CSV file from S3", st.session_state.uploaded_files)
    else:
        selected_filename = st.text_input("Enter CSV filename (e.g., sales_data/2025/07/03_1.csv)")
    if st.button("Retrieve Data"):
        try:
            response = requests.get(f"http://localhost:8000/data/get/{selected_filename}", headers=headers)
            if response.status_code == 200:
                df = pd.DataFrame(response.json())
                st.subheader("Retrieved Sales Data")
                st.dataframe(df.head(), use_container_width=True)
                
                # Plot retrieved data
                st.subheader("Retrieved Sales Trend")
                df["date"] = pd.to_datetime(df["date"])
                fig = px.line(df, x="date", y="quantity", color="product_id", title="Retrieved Sales Quantity Over Time")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.error(f"Retrieval failed: {response.json().get('detail', 'Unknown error')}")
        except Exception as e:
            st.error(f"Error retrieving data: {str(e)}")

    # Forecast sales data
    st.header("Forecast Sales Data and Inventory Optimization")
    if "uploaded_files" in st.session_state and st.session_state.uploaded_files:
        forecast_filename = st.selectbox("Select a CSV file to forecast", st.session_state.uploaded_files, key="forecast_select")
    else:
        forecast_filename = st.text_input("Enter CSV filename to forecast (e.g., sales_data/2025/07/03_1.csv)")
    if st.button("Generate Forecast and Inventory Recommendations"):
        try:
            response = requests.get(f"http://localhost:8000/data/forecast/{forecast_filename}", headers=headers)
            if response.status_code == 200:
                forecast_df = pd.DataFrame(response.json().get("forecast"))
                inventory_df = pd.DataFrame(response.json().get("inventory"))
                forecast_s3_path = response.json().get("forecast_s3_path")
                inventory_s3_path = response.json().get("inventory_s3_path")
                
                # Display warning if any products were skipped
                if "warning" in response.json():
                    st.warning(response.json().get("warning"))
                
                # Display forecast
                st.subheader("Sales Forecast (Next 30 Days)")
                st.dataframe(forecast_df.head(), use_container_width=True)
                
                # Plot forecast
                st.subheader("Forecast Trend by Product")
                forecast_df["ds"] = pd.to_datetime(forecast_df["ds"])
                fig = px.line(
                    forecast_df,
                    x="ds",
                    y="yhat",
                    color="product_id",
                    title="Sales Forecast by Product (Next 30 Days)",
                    labels={"ds": "Date", "yhat": "Predicted Quantity"}
                )
                fig.add_scatter(
                    x=forecast_df["ds"],
                    y=forecast_df["yhat_lower"],
                    mode="lines",
                    name="Lower Bound",
                    line=dict(dash="dash")
                )
                fig.add_scatter(
                    x=forecast_df["ds"],
                    y=forecast_df["yhat_upper"],
                    mode="lines",
                    name="Upper Bound",
                    line=dict(dash="dash")
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Display inventory recommendations
                st.subheader("Inventory Recommendations")
                st.dataframe(inventory_df, use_container_width=True)
                
                st.success(f"Forecast stored at S3: {forecast_s3_path}")
                st.success(f"Inventory recommendations stored at S3: {inventory_s3_path}")
            else:
                st.error(f"Forecast failed: {response.json().get('detail', 'Unknown error')}")
        except Exception as e:
            st.error(f"Error generating forecast: {str(e)}")