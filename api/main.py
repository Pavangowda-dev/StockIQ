from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from api.routes import data
from api.auth import get_current_user, create_access_token, verify_password, get_user
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="StockIQ API",
    description="Backend for StockIQ: TimeLLM-powered supply chain optimization",
    version="0.1.0"
)

# Include routes with authentication
app.include_router(data.router, dependencies=[Depends(get_current_user)])

@app.get("/")
async def root():
    return {"message": "Welcome to StockIQ API"}

@app.post("/auth/token")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = get_user(form_data.username)
    if not user or not verify_password(form_data.password, user.hashed_password):
        logger.error(f"Failed login attempt for user: {form_data.username}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token = create_access_token(data={"sub": form_data.username})
    logger.info(f"User {form_data.username} logged in successfully")
    return {"access_token": access_token, "token_type": "bearer"}