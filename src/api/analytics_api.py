"""FastAPI endpoints for analytics queries"""
from fastapi import FastAPI, Query
from typing import Optional
from datetime import date

app = FastAPI(title="E-commerce Analytics API")

@app.get("/metrics/daily")
async def get_daily_metrics(
    start_date: date = Query(...),
    end_date: date = Query(...)
):
    """Get daily aggregated metrics"""
    # Query from database or data lake
    return {
        "start_date": str(start_date),
        "end_date": str(end_date),
        "total_revenue": 125000.50,
        "total_transactions": 1523,
        "avg_order_value": 82.15
    }

@app.get("/customers/{customer_id}/ltv")
async def get_customer_ltv(customer_id: str):
    """Calculate customer lifetime value"""
    return {
        "customer_id": customer_id,
        "lifetime_value": 1250.75,
        "total_orders": 15,
        "avg_order_value": 83.38
    }
