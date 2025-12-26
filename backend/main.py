"""
YXDB to SQL Converter - FastAPI Backend
Main application entry point
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv
import os

from routes.conversion import router as conversion_router
from routes.query import router as query_router
from routes.admin import router as admin_router
from routes.population import router as population_router
from routes.reporting import router as reporting_router
from routes.messaging import router as messaging_router
from services.middleware import RequestLoggingMiddleware

# Load environment variables
load_dotenv()

app = FastAPI(
    title="YXDB to SQL Converter",
    description="Convert Alteryx .yxdb files to SQL databases with NLP query capabilities",
    version="1.1.0"
)

# CORS configuration - allow all origins in development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for development
    allow_credentials=False,  # Must be False when using "*"
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(conversion_router, prefix="/api", tags=["Conversion"])
app.include_router(query_router, prefix="/api", tags=["Query"])
app.include_router(admin_router, prefix="/api", tags=["Admin"])
app.include_router(population_router, prefix="/api", tags=["Populations"])
app.include_router(reporting_router, prefix="/api", tags=["Reporting"])
app.include_router(messaging_router, prefix="/api", tags=["Messaging"])

# Create uploads and databases directories
os.makedirs("uploads", exist_ok=True)
os.makedirs("databases", exist_ok=True)
os.makedirs("reports", exist_ok=True)
os.makedirs("messages", exist_ok=True)



@app.get("/")
async def root():
    return {
        "message": "YXDB to SQL Converter API",
        "docs": "/docs",
        "version": "1.0.0"
    }


@app.get("/api/health")
async def health_check():
    return {"status": "healthy"}
