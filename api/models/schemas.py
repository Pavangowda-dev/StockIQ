from pydantic import BaseModel
from typing import Optional

class SalesData(BaseModel):
    date: str
    product_id: str
    quantity: int