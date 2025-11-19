"""
FastAPI REST API for E-commerce Analytics
Provides endpoints for querying analytics data
"""
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="E-commerce Analytics API",
    description="Real-time analytics and reporting API",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class AnalyticsQuery(BaseModel):
    start_date: datetime
    end_date: datetime
    metric: str
    granularity: Optional[str] = "hourly"

class KPIResponse(BaseModel):
    metric_name: str
    value: float
    timestamp: datetime
    period: str

@app.get("/")
async def root():
    return {
        "message": "E-commerce Analytics API",
        "version": "1.0.0",
        "status": "healthy"
    }

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/kpis/realtime", response_model=List[KPIResponse])
async def get_realtime_kpis():
    """Get real-time KPIs for the last 24 hours"""
    try:
        # Mock data - replace with actual database query
        kpis = [
            KPIResponse(
                metric_name="total_revenue",
                value=125000.50,
                timestamp=datetime.now(),
                period="24h"
            ),
            KPIResponse(
                metric_name="active_users",
                value=5432,
                timestamp=datetime.now(),
                period="24h"
            ),
            KPIResponse(
                metric_name="conversion_rate",
                value=3.45,
                timestamp=datetime.now(),
                period="24h"
            )
        ]
        return kpis
    except Exception as e:
        logger.error(f"Error fetching KPIs: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/analytics/query")
async def query_analytics(query: AnalyticsQuery):
    """Query historical analytics data"""
    try:
        # Validate date range
        if query.end_date < query.start_date:
            raise HTTPException(status_code=400, detail="Invalid date range")
        
        # Mock response - replace with actual query logic
        return {
            "query": query.dict(),
            "results": [],
            "count": 0,
            "execution_time_ms": 125
        }
    except Exception as e:
        logger.error(f"Error querying analytics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
