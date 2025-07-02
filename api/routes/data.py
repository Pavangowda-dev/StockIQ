from fastapi import APIRouter, UploadFile, File, HTTPException
import pandas as pd
import io
import boto3
from botocore.exceptions import ClientError
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/data", tags=["data"])

# Replace with your actual S3 bucket name
BUCKET_NAME = "stockiq-data-<your-username>"  # Update with your bucket name
s3_client = boto3.client("s3")

@router.post("/upload")
async def upload_sales_data(file: UploadFile = File(...)):
    try:
        # Validate file extension
        if not file.filename.endswith(".csv"):
            logger.error(f"Invalid file format: {file.filename}")
            raise HTTPException(status_code=400, detail="Only CSV files are supported")
        
        # Read CSV file
        logger.info(f"Processing file: {file.filename}")
        content = await file.read()
        df = pd.read_csv(io.BytesIO(content))
        
        # Validate columns
        required_columns = ["date", "product_id", "quantity"]
        if not all(col in df.columns for col in required_columns):
            missing_cols = [col for col in required_columns if col not in df.columns]
            logger.error(f"Missing columns: {missing_cols}")
            raise HTTPException(status_code=400, detail=f"CSV must contain columns: {', '.join(required_columns)}")
        
        # Upload to S3
        logger.info(f"Uploading {file.filename} to S3 bucket: {BUCKET_NAME}")
        s3_client.upload_fileobj(
            io.BytesIO(content),
            BUCKET_NAME,
            f"sales_data/{file.filename}"
        )
        
        logger.info(f"Successfully uploaded {file.filename} with {len(df)} rows")
        return {"message": f"Uploaded {file.filename} with {len(df)} rows to S3"}
    
    except pd.errors.ParserError as e:
        logger.error(f"CSV parsing error: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Invalid CSV format: {str(e)}")
    except ClientError as e:
        logger.error(f"S3 upload error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to upload to S3: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")