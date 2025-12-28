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
from routes.scheduler import router as scheduler_router
from routes.database import router as database_router
from routes.ai_leads import router as ai_leads_router
from routes.auth import router as auth_router
from routes.websocket import router as websocket_router
from routes.scheduled_imports import router as scheduled_imports_router
from routes.campaigns import router as campaigns_router
from routes.integrations import router as integrations_router
from routes.compliance import router as compliance_router
from routes.dashboard_builder import router as dashboard_builder_router
# Phase 211-310 routers
from routes.analytics import router as analytics_router
from routes.workflows import router as workflows_router
from routes.ai_assistant import router as ai_assistant_router
from routes.sso import router as sso_router
from routes.marketplace import router as marketplace_router
from routes.developer import router as developer_router
from routes.billing import router as billing_router
from routes.advanced_reports import router as advanced_reports_router
from routes.monitoring import router as monitoring_router
# Phase 311-410 routers
from routes.data_science import router as data_science_router
from routes.customer_success import router as customer_success_router
from routes.white_label import router as white_label_router
from routes.api_gateway import router as api_gateway_router
from routes.event_streaming import router as event_streaming_router
from routes.knowledge_graph import router as knowledge_graph_router
from routes.mlops import router as mlops_router
from routes.predictive_ux import router as predictive_ux_router
from routes.distributed import router as distributed_router
from routes.global_deployment import router as global_deployment_router
from services.middleware import RequestLoggingMiddleware

# Load environment variables
load_dotenv()

app = FastAPI(
    title="ACA DataHub - AI Analytics Platform",
    description="Enterprise data analytics with AI-powered lead scoring, NLP queries, campaigns, integrations, and compliance",
    version="3.0.0"
)

# CORS configuration - allow all origins in development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for development
    allow_credentials=False,  # Must be False when using "*"
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers - Core
app.include_router(conversion_router, prefix="/api", tags=["Conversion"])
app.include_router(query_router, prefix="/api", tags=["Query"])
app.include_router(admin_router, prefix="/api", tags=["Admin"])
app.include_router(population_router, prefix="/api", tags=["Populations"])
app.include_router(reporting_router, prefix="/api", tags=["Reporting"])
app.include_router(messaging_router, prefix="/api", tags=["Messaging"])
app.include_router(scheduler_router, prefix="/api", tags=["Scheduler"])
app.include_router(database_router, prefix="/api", tags=["Database"])
app.include_router(ai_leads_router, tags=["AI Leads"])

# Include routers - Enterprise (Phases 111-210)
app.include_router(auth_router, prefix="/api", tags=["Authentication"])
app.include_router(websocket_router, tags=["WebSocket"])
app.include_router(scheduled_imports_router, prefix="/api", tags=["Scheduled Imports"])
app.include_router(campaigns_router, prefix="/api", tags=["Campaigns"])
app.include_router(integrations_router, prefix="/api", tags=["Integrations"])
app.include_router(compliance_router, prefix="/api", tags=["Compliance"])
app.include_router(dashboard_builder_router, prefix="/api", tags=["Dashboards"])

# Include routers - Advanced (Phases 211-310)
app.include_router(analytics_router, prefix="/api", tags=["Advanced Analytics"])
app.include_router(workflows_router, prefix="/api", tags=["Workflows"])
app.include_router(ai_assistant_router, prefix="/api", tags=["AI Assistant"])
app.include_router(sso_router, prefix="/api", tags=["Enterprise SSO"])
app.include_router(marketplace_router, prefix="/api", tags=["Data Marketplace"])
app.include_router(developer_router, prefix="/api", tags=["Developer Platform"])
app.include_router(billing_router, prefix="/api", tags=["Billing"])
app.include_router(advanced_reports_router, prefix="/api", tags=["Advanced Reports"])
app.include_router(monitoring_router, prefix="/api", tags=["Monitoring"])

# Include routers - Next-Gen (Phases 311-410)
app.include_router(data_science_router, prefix="/api", tags=["Data Science"])
app.include_router(customer_success_router, prefix="/api", tags=["Customer Success"])
app.include_router(white_label_router, prefix="/api", tags=["White-Label"])
app.include_router(api_gateway_router, prefix="/api", tags=["API Gateway"])
app.include_router(event_streaming_router, prefix="/api", tags=["Event Streaming"])
app.include_router(knowledge_graph_router, prefix="/api", tags=["Knowledge Graph"])
app.include_router(mlops_router, prefix="/api", tags=["MLOps"])
app.include_router(predictive_ux_router, prefix="/api", tags=["Predictive UX"])
app.include_router(distributed_router, prefix="/api", tags=["Distributed Computing"])
app.include_router(global_deployment_router, prefix="/api", tags=["Global Deployment"])


# Create uploads and databases directories
os.makedirs("uploads", exist_ok=True)
os.makedirs("databases", exist_ok=True)
os.makedirs("reports", exist_ok=True)
os.makedirs("messages", exist_ok=True)



@app.get("/")
async def root():
    return {
        "message": "ACA DataHub - Enterprise AI Analytics Platform",
        "docs": "/docs",
        "version": "5.0.0",
        "phases_completed": 410
    }


@app.get("/api/health")
async def health_check():
    return {"status": "healthy"}


