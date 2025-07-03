from fastapi import APIRouter, UploadFile, File, HTTPException
import pandas as pd
import io
import boto3
from botocore.exceptions import ClientError
import logging
from datetime import datetime
from prophet import Prophet

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/data", tags=["data"])

# Replace with your actual S3 bucket name
BUCKET_NAME = "stockiq-data-pavan"  # Update with your actual bucket name
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
        
        # Generate unique S3 filename with new format (e.g., 2025/07/03_1.csv)
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
        
        # Prepare data for Prophet (aggregate by date for simplicity)
        df["date"] = pd.to_datetime(df["date"])
        df_agg = df.groupby("date")["quantity"].sum().reset_index()
        df_agg = df_agg.rename(columns={"date": "ds", "quantity": "y"})
        
        # Initialize and fit Prophet model
        model = Prophet(yearly_seasonality=False, weekly_seasonality=True, daily_seasonality=True)
        model.fit(df_agg)
        
        # Create future dataframe for next 30 days
        future = model.make_future_dataframe(periods=30)
        forecast = model.predict(future)
        
        # Select relevant columns
        forecast = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]]
        forecast["ds"] = forecast["ds"].dt.strftime("%Y-%m-%d")
        
        # Save forecast to S3
        forecast_filename = s3_key.replace("sales_data/", "forecasts/").replace(".csv", "_forecast.csv")
        csv_buffer = io.StringIO()
        forecast.to_csv(csv_buffer, index=False)
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=forecast_filename,
            Body=csv_buffer.getvalue()
        )
        logger.info(f"Stored forecast at {forecast_filename}")
        
        # Return forecast as JSON
        return {
            "message": f"Forecast generated for {filename}",
            "forecast": forecast.to_dict(orient="records"),
            "forecast_s3_path": forecast_filename
        }
    
    except ClientError as e:
        logger.error(f"S3 retrieval error: {str(e)}")
        if e.response["Error"]["Code"] == "NoSuchKey":
            raise HTTPException(status_code=404, detail="File not found in S3")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve from S3: {str(e)}")
    except Exception as e:
        logger.error(f"Forecasting error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")