import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import io

# Streamlit page configuration
st.set_page_config(page_title="StockIQ", layout="wide", page_icon="📦")

# Title and description
st.title("StockIQ: AI-Powered Supply Chain Optimization")
st.markdown("Upload sales data to predict demand and optimize inventory.")

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
                response = requests.post("http://localhost:8000/data/upload", files=files)
                
                if response.status_code == 200:
                    st.success(response.json().get("message", "File uploaded successfully"))
                else:
                    st.error(f"Upload failed: {response.json().get('detail', 'Unknown error')}")
            except Exception as e:
                st.error(f"Error connecting to server: {str(e)}")
    else:
        st.error(f"CSV must contain columns: {', '.join(required_columns)}")

# Retrieve data from S3
st.header("Retrieve Sales Data from S3")
filename = st.text_input("Enter CSV filename (e.g., sample_sales.csv)")
if st.button("Retrieve Data"):
    try:
        response = requests.get(f"http://localhost:8000/data/get/{filename}")
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