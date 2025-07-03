from fastapi import APIRouter, UploadFile, File, HTTPException
import pandas as pd
import io
import boto3
from botocore.exceptions import ClientError
import logging
from datetime import datetime, timedelta
from prophet import Prophet

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/data", tags=["data"])

# Replace with your actual S3 bucket name
BUCKET_NAME = "stockiq-data-pavan"  # Correct bucket name
s3_client = boto3.client("s3")

@router.post("/upload")
async def upload_sales_data(file: UploadFile = File(...)):
    try:
        # Validate file extension
        if not file.filename.endswith(".csv"):
            logger.error(f"Invalid file format: {file.filename}")
            raise HTTPException(status_code=400, detail="Only CSV files are supported")
        
        # Read file content
        logger.info(f"Processing file: {file.filename}")
        content = await file.read()
        
        # Debug: Check file content
        if not content:
            logger.error("Received empty file")
            raise HTTPException(status_code=400, detail="Empty file uploaded")
        
        logger.info(f"File size: {len(content)} bytes")
        
        # Parse CSV
        try:
            df = pd.read_csv(io.BytesIO(content))
        except pd.errors.EmptyDataError:
            logger.error("CSV file is empty or has no columns")
            raise HTTPException(status_code=400, detail="No columns to parse from file")
        except pd.errors.ParserError as e:
            logger.error(f"CSV parsing error: {str(e)}")
            raise HTTPException(status_code=400, detail=f"Invalid CSV format: {str(e)}")
        
        # Validate columns
        required_columns = ["date", "product_id", "quantity"]
        if not all(col in df.columns for col in required_columns):
            missing_cols = [col for col in required_columns if col not in df.columns]
            logger.error(f"Missing columns: {missing_cols}")
            raise HTTPException(status_code=400, detail=f"CSV must contain columns: {', '.join(required_columns)}")
        
        # Generate unique S3 filename with format (e.g., sales_data/2025/07/03_1.csv)
        today = datetime.now()
        date_path = today.strftime("%Y/%m/%d")
        try:
            response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=f"sales_data/{date_path}_")
            file_count = len(response.get("Contents", [])) + 1 if response.get("Contents") else 1
        except ClientError as e:
            logger.error(f"Error listing S3 objects: {str(e)}")
            file_count = 1
        s3_filename = f"sales_data/{date_path}_{file_count}.csv"
        
        # Upload to S3
        logger.info(f"Uploading {s3_filename} to S3 bucket: {BUCKET_NAME}")
        try:
            s3_client.upload_fileobj(
                io.BytesIO(content),
                BUCKET_NAME,
                s3_filename
            )
        except ClientError as e:
            logger.error(f"S3 upload error: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Failed to upload to S3: {str(e)}")
        
        logger.info(f"Successfully uploaded {s3_filename} with {len(df)} rows")
        return {"message": f"Uploaded {file.filename} with {len(df)} rows to S3", "s3_filename": s3_filename}
    
    except pd.errors.EmptyDataError as e:
        logger.error(f"CSV parsing error: {str(e)}")
        raise HTTPException(status_code=400, detail=f"No columns to parse from file")
    except pd.errors.ParserError as e:
        logger.error(f"CSV parsing error: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Invalid CSV format: {str(e)}")
    except ClientError as e:
        logger.error(f"S3 upload error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to upload to S3: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get("/get/{filename:path}")
async def get_sales_data(filename: str):
    try:
        # Validate filename
        if not filename.endswith(".csv"):
            logger.error(f"Invalid file format: {filename}")
            raise HTTPException(status_code=400, detail="Only CSV files are supported")
        
        # Ensure filename includes sales_data/ prefix
        s3_key = filename if filename.startswith("sales_data/") else f"sales_data/{filename}"
        logger.info(f"Retrieving {s3_key} from S3 bucket: {BUCKET_NAME}")
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        content = response["Body"].read()
        df = pd.read_csv(io.BytesIO(content))
        
        # Convert DataFrame to JSON
        return df.to_dict(orient="records")
    
    except ClientError as e:
        logger.error(f"S3 retrieval error: {str(e)}")
        if e.response["Error"]["Code"] == "NoSuchKey":
            raise HTTPException(status_code=404, detail="File not found in S3")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve from S3: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get("/forecast/{filename:path}")
async def forecast_sales_data(filename: str):
    try:
        # Validate filename
        if not filename.endswith(".csv"):
            logger.error(f"Invalid file format: {filename}")
            raise HTTPException(status_code=400, detail="Only CSV files are supported")
        
        # Retrieve data from S3
        s3_key = filename if filename.startswith("sales_data/") else f"sales_data/{filename}"
        logger.info(f"Retrieving {s3_key} from S3 bucket: {BUCKET_NAME}")
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        content = response["Body"].read()
        df = pd.read_csv(io.BytesIO(content))
        
        # Validate columns
        required_columns = ["date", "product_id", "quantity"]
        if not all(col in df.columns for col in required_columns):
            missing_cols = [col for col in required_columns if col not in df.columns]
            logger.error(f"Missing columns: {missing_cols}")
            raise HTTPException(status_code=400, detail=f"CSV must contain columns: {', '.join(required_columns)}")
        
        # Check for NaN values
        if df[["date", "quantity"]].isna().any().any():
            logger.error("Data contains NaN values in date or quantity columns")
            raise HTTPException(status_code=400, detail="Data contains NaN values in date or quantity columns")
        
        # Prepare data for Prophet (per product)
        df["date"] = pd.to_datetime(df["date"])
        products = df["product_id"].unique()
        forecasts = []
        inventory_recommendations = []
        lead_time_days = 7  # Assumed lead time
        safety_stock_factor = 1.5  # 50% buffer for variability
        skipped_products = []
        
        for product in products:
            # Filter data for the product
            df_product = df[df["product_id"] == product][["date", "quantity"]].rename(columns={"date": "ds", "quantity": "y"})
            
            # Check if enough data points (at least 2 non-NaN rows)
            if len(df_product) < 2:
                logger.warning(f"Skipping product {product}: insufficient data points ({len(df_product)} rows)")
                skipped_products.append(product)
                continue
            
            # Initialize and fit Prophet model with fine-tuned parameters
            model = Prophet(
                yearly_seasonality=True,
                weekly_seasonality=True,
                daily_seasonality=True,
                seasonality_mode="multiplicative",  # Better for varying trends
                changepoint_prior_scale=0.05  # Adjust for flexibility in trend changes
            )
            model.add_country_holidays(country_name="US")  # Add US holidays
            model.fit(df_product)
            
            # Create future dataframe for next 30 days
            future = model.make_future_dataframe(periods=30)
            forecast = model.predict(future)
            
            # Select relevant columns
            forecast = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]]
            forecast["product_id"] = product
            forecasts.append(forecast)
            
            # Calculate inventory recommendations (reorder point)
            lead_time_demand = forecast[forecast["ds"] > df["date"].max()].head(lead_time_days)["yhat"].sum()
            safety_stock = lead_time_demand * safety_stock_factor
            reorder_point = lead_time_demand + safety_stock
            inventory_recommendations.append({
                "product_id": product,
                "lead_time_demand": round(lead_time_demand, 2),
                "safety_stock": round(safety_stock, 2),
                "reorder_point": round(reorder_point, 2)
            })
        
        if not forecasts:
            logger.error("No products have sufficient data for forecasting")
            raise HTTPException(status_code=400, detail="No products have at least 2 data points for forecasting")
        
        # Combine forecasts
        forecast_df = pd.concat(forecasts, ignore_index=True)
        forecast_df["ds"] = forecast_df["ds"].dt.strftime("%Y-%m-%d")
        
        # Save forecast to S3
        forecast_filename = s3_key.replace("sales_data/", "forecasts/").replace(".csv", "_forecast.csv")
        csv_buffer = io.StringIO()
        forecast_df.to_csv(csv_buffer, index=False)
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=forecast_filename,
            Body=csv_buffer.getvalue()
        )
        logger.info(f"Stored forecast at {forecast_filename}")
        
        # Save inventory recommendations to S3
        inventory_filename = s3_key.replace("sales_data/", "inventory/").replace(".csv", "_inventory.csv")
        inventory_df = pd.DataFrame(inventory_recommendations)
        csv_buffer = io.StringIO()
        inventory_df.to_csv(csv_buffer, index=False)
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=inventory_filename,
            Body=csv_buffer.getvalue()
        )
        logger.info(f"Stored inventory recommendations at {inventory_filename}")
        
        # Return forecast and inventory recommendations
        response = {
            "message": f"Forecast and inventory recommendations generated for {filename}",
            "forecast": forecast_df.to_dict(orient="records"),
            "inventory": inventory_df.to_dict(orient="records"),
            "forecast_s3_path": forecast_filename,
            "inventory_s3_path": inventory_filename
        }
        if skipped_products:
            response["warning"] = f"Skipped products due to insufficient data: {', '.join(skipped_products)}"
        
        return response
    
    except ClientError as e:
        logger.error(f"S3 retrieval error: {str(e)}")
        if e.response["Error"]["Code"] == "NoSuchKey":
            raise HTTPException(status_code=404, detail="File not found in S3")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve from S3: {str(e)}")
    except Exception as e:
        logger.error(f"Forecasting error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")