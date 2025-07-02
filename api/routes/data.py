from fastapi import APIRouter, UploadFile, File
import pandas as pd
import io

router = APIRouter(prefix="/data", tags=["data"])

@router.post("/upload")
async def upload_sales_data(file: UploadFile = File(...)):
    if not file.filename.endswith(".csv"):
        return {"error": "Only CSV files are supported"}
    
    # Read CSV file
    content = await file.read()
    df = pd.read_csv(io.BytesIO(content))
    
    # Basic validation
    required_columns = ["date", "product_id", "quantity"]
    if not all(col in df.columns for col in required_columns):
        return {"error": f"CSV must contain columns: {', '.join(required_columns)}"}
    
    # Placeholder: Process data (to be expanded later)
    return {"message": f"Uploaded {file.filename} with {len(df)} rows"}