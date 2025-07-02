import pandas as pd

def validate_sales_data(df: pd.DataFrame) -> bool:
    """Validate sales DataFrame for required columns and data types."""
    required_columns = ["date", "product_id", "quantity"]
    if not all(col in df.columns for col in required_columns):
        return False
    try:
        df["date"] = pd.to_datetime(df["date"])
        df["quantity"] = pd.to_numeric(df["quantity"])
        return True
    except Exception:
        return False