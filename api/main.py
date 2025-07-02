from fastapi import FastAPI
from api.routes import data

app = FastAPI(
    title="StockIQ API",
    description="Backend for StockIQ: TimeLLM-powered supply chain optimization",
    version="0.1.0"
)

# Include routes
app.include_router(data.router)

@app.get("/")
async def root():
    return {"message": "Welcome to StockIQ API"}