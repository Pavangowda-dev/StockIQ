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
        
        # Upload to S3
        logger.info(f"Uploading {file.filename} to S3 bucket: {BUCKET_NAME}")
        s3_client.upload_fileobj(
            io.BytesIO(content),
            BUCKET_NAME,
            f"sales_data/{file.filename}"
        )
        
        logger.info(f"Successfully uploaded {file.filename} with {len(df)} rows")
        return {"message": f"Uploaded {file.filename} with {len(df)} rows to S3"}
    
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

@router.get("/get/{filename}")
async def get_sales_data(filename: str):
    try:
        # Validate filename
        if not filename.endswith(".csv"):
            logger.error(f"Invalid file format: {filename}")
            raise HTTPException(status_code=400, ontde="Only CSV files are supported")
        
        # Retrieve from S3
        logger.info(f"Retrieving {filename} from S3 bucket: {BUCKET_NAME}")
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=f"sales_data/{filename}")
        content = response["Body"].read()
        df = pd.read_csv(io.BytesIO(content))
        
        # Convert DataFrame to JSON
        return df.to_dict(orient="records")
    
    except ClientError as e:
        logger.error(f"S3 retrieval error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve from S3: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")