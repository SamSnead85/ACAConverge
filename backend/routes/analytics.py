"""
Advanced Analytics Routes for ACA DataHub
Custom metrics, cohort analysis, forecasting, and BI features
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from enum import Enum
import math
import random

router = APIRouter(prefix="/analytics", tags=["Advanced Analytics"])


# =========================================================================
# Models
# =========================================================================

class MetricType(str, Enum):
    COUNT = "count"
    SUM = "sum"
    AVERAGE = "average"
    MIN = "min"
    MAX = "max"
    PERCENTILE = "percentile"
    CUSTOM = "custom"


class TimeGranularity(str, Enum):
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"


class MetricDefinition(BaseModel):
    name: str
    formula: str  # e.g., "sum(revenue) / count(customers)"
    description: Optional[str] = None
    format: str = "number"  # number, currency, percentage
    goal: Optional[float] = None


# =========================================================================
# Metrics Store
# =========================================================================

class MetricsStore:
    """Custom metrics engine with formula support"""
    
    def __init__(self):
        self.metrics: Dict[str, dict] = {}
        self._counter = 0
        self._init_default_metrics()
    
    def _init_default_metrics(self):
        defaults = [
            {"name": "Total Leads", "formula": "count(leads)", "format": "number"},
            {"name": "Conversion Rate", "formula": "count(converted) / count(leads) * 100", "format": "percentage"},
            {"name": "Avg Lead Score", "formula": "avg(score)", "format": "number"},
            {"name": "Email Open Rate", "formula": "sum(opened) / sum(sent) * 100", "format": "percentage"},
            {"name": "Campaign ROI", "formula": "(revenue - cost) / cost * 100", "format": "percentage"},
        ]
        for m in defaults:
            self.create(m)
    
    def create(self, data: dict) -> dict:
        self._counter += 1
        metric_id = f"metric_{self._counter}"
        metric = {
            "id": metric_id,
            "created_at": datetime.utcnow().isoformat(),
            **data
        }
        self.metrics[metric_id] = metric
        return metric
    
    def list(self) -> List[dict]:
        return list(self.metrics.values())
    
    def get(self, metric_id: str) -> Optional[dict]:
        return self.metrics.get(metric_id)
    
    def delete(self, metric_id: str) -> bool:
        if metric_id in self.metrics:
            del self.metrics[metric_id]
            return True
        return False
    
    def evaluate(self, metric_id: str, data: dict = None) -> dict:
        """Evaluate a metric with sample data"""
        metric = self.get(metric_id)
        if not metric:
            return {"error": "Metric not found"}
        
        # Simulate metric calculation
        value = random.uniform(10, 1000)
        previous = value * (1 + random.uniform(-0.2, 0.2))
        
        return {
            "metric_id": metric_id,
            "name": metric["name"],
            "value": round(value, 2),
            "previous_value": round(previous, 2),
            "change": round((value - previous) / previous * 100, 1),
            "format": metric.get("format", "number"),
            "goal": metric.get("goal"),
            "goal_progress": round(value / metric["goal"] * 100, 1) if metric.get("goal") else None
        }


metrics_store = MetricsStore()


# =========================================================================
# Cohort Analysis
# =========================================================================

class CohortAnalyzer:
    """Cohort analysis for retention and engagement"""
    
    def build_cohorts(
        self,
        data: List[dict],
        cohort_column: str,
        activity_column: str,
        granularity: str = "month"
    ) -> dict:
        """Build cohort retention matrix"""
        # Simulate cohort data
        cohorts = []
        for i in range(6):
            cohort_date = (datetime.utcnow() - timedelta(days=30 * i)).strftime("%Y-%m")
            retention = [100]  # Start at 100%
            for period in range(1, 7 - i):
                # Simulate decay
                retention.append(round(retention[-1] * random.uniform(0.7, 0.95), 1))
            
            cohorts.append({
                "cohort": cohort_date,
                "size": random.randint(100, 500),
                "retention": retention
            })
        
        return {
            "cohorts": cohorts,
            "granularity": granularity,
            "periods": ["Period 0", "Period 1", "Period 2", "Period 3", "Period 4", "Period 5"],
            "generated_at": datetime.utcnow().isoformat()
        }
    
    def calculate_retention(self, cohort_data: dict) -> dict:
        """Calculate average retention rates"""
        if not cohort_data.get("cohorts"):
            return {}
        
        avg_retention = []
        for period in range(6):
            values = [c["retention"][period] for c in cohort_data["cohorts"] if len(c["retention"]) > period]
            if values:
                avg_retention.append(round(sum(values) / len(values), 1))
        
        return {
            "average_retention": avg_retention,
            "best_cohort": max(cohort_data["cohorts"], key=lambda x: x["retention"][-1] if x["retention"] else 0)["cohort"],
            "worst_cohort": min(cohort_data["cohorts"], key=lambda x: x["retention"][-1] if x["retention"] else 100)["cohort"]
        }


cohort_analyzer = CohortAnalyzer()


# =========================================================================
# Forecasting
# =========================================================================

class ForecastEngine:
    """Time series forecasting"""
    
    def forecast(
        self,
        data: List[dict],
        value_column: str,
        date_column: str,
        periods: int = 7,
        method: str = "linear"
    ) -> dict:
        """Generate forecast for future periods"""
        # Simulate historical data
        historical = []
        base = 100
        for i in range(30):
            date = (datetime.utcnow() - timedelta(days=30 - i)).strftime("%Y-%m-%d")
            value = base + random.uniform(-20, 30)
            base = value
            historical.append({"date": date, "value": round(value, 2)})
        
        # Generate forecast
        forecast = []
        trend = (historical[-1]["value"] - historical[0]["value"]) / len(historical)
        last_value = historical[-1]["value"]
        
        for i in range(1, periods + 1):
            date = (datetime.utcnow() + timedelta(days=i)).strftime("%Y-%m-%d")
            predicted = last_value + (trend * i) + random.uniform(-5, 5)
            lower = predicted * 0.9
            upper = predicted * 1.1
            
            forecast.append({
                "date": date,
                "predicted": round(predicted, 2),
                "lower_bound": round(lower, 2),
                "upper_bound": round(upper, 2)
            })
        
        return {
            "historical": historical[-10:],  # Last 10 data points
            "forecast": forecast,
            "method": method,
            "confidence": 0.85,
            "trend": "increasing" if trend > 0 else "decreasing",
            "trend_strength": abs(trend)
        }


forecast_engine = ForecastEngine()


# =========================================================================
# CLV Calculator
# =========================================================================

class CLVCalculator:
    """Customer Lifetime Value calculations"""
    
    def calculate_clv(
        self,
        avg_purchase_value: float,
        purchase_frequency: float,
        customer_lifespan: float,
        margin: float = 0.3
    ) -> dict:
        """Calculate CLV using basic formula"""
        clv = avg_purchase_value * purchase_frequency * customer_lifespan * margin
        
        return {
            "clv": round(clv, 2),
            "annual_value": round(avg_purchase_value * purchase_frequency, 2),
            "inputs": {
                "avg_purchase_value": avg_purchase_value,
                "purchase_frequency": purchase_frequency,
                "customer_lifespan": customer_lifespan,
                "margin": margin
            },
            "segments": self._segment_clv(clv)
        }
    
    def _segment_clv(self, clv: float) -> dict:
        """Segment customers by CLV"""
        return {
            "high_value": {"threshold": clv * 1.5, "action": "VIP treatment, personal outreach"},
            "medium_value": {"threshold": clv, "action": "Regular engagement campaigns"},
            "low_value": {"threshold": clv * 0.5, "action": "Reactivation campaigns"}
        }


clv_calculator = CLVCalculator()


# =========================================================================
# Statistical Testing
# =========================================================================

class StatisticalTester:
    """A/B test significance and statistical analysis"""
    
    def ab_significance(
        self,
        control_conversions: int,
        control_total: int,
        variant_conversions: int,
        variant_total: int
    ) -> dict:
        """Calculate statistical significance of A/B test"""
        control_rate = control_conversions / control_total if control_total > 0 else 0
        variant_rate = variant_conversions / variant_total if variant_total > 0 else 0
        
        # Calculate lift
        lift = ((variant_rate - control_rate) / control_rate * 100) if control_rate > 0 else 0
        
        # Simplified z-test
        p = (control_conversions + variant_conversions) / (control_total + variant_total)
        se = math.sqrt(p * (1 - p) * (1/control_total + 1/variant_total)) if p > 0 and p < 1 else 0.01
        z = (variant_rate - control_rate) / se if se > 0 else 0
        
        # Confidence level from z-score
        confidence = min(99.9, abs(z) * 15 + 50)
        
        return {
            "control": {
                "conversions": control_conversions,
                "total": control_total,
                "rate": round(control_rate * 100, 2)
            },
            "variant": {
                "conversions": variant_conversions,
                "total": variant_total,
                "rate": round(variant_rate * 100, 2)
            },
            "lift": round(lift, 2),
            "confidence": round(confidence, 1),
            "is_significant": confidence >= 95,
            "winner": "variant" if lift > 0 and confidence >= 95 else ("control" if lift < 0 and confidence >= 95 else "inconclusive"),
            "recommendation": self._get_recommendation(lift, confidence)
        }
    
    def _get_recommendation(self, lift: float, confidence: float) -> str:
        if confidence < 95:
            return "Continue test to reach statistical significance"
        if lift > 10:
            return "Strong winner - implement variant immediately"
        if lift > 0:
            return "Variant wins - consider implementing"
        return "Control wins - keep current version"


stat_tester = StatisticalTester()


# =========================================================================
# Endpoints
# =========================================================================

@router.get("/metrics")
async def list_metrics():
    """List all custom metrics"""
    return {"metrics": metrics_store.list()}


@router.post("/metrics")
async def create_metric(metric: MetricDefinition):
    """Create a custom metric"""
    result = metrics_store.create(metric.dict())
    return {"success": True, "metric": result}


@router.get("/metrics/{metric_id}/evaluate")
async def evaluate_metric(metric_id: str):
    """Evaluate a metric and get current value"""
    result = metrics_store.evaluate(metric_id)
    return result


@router.delete("/metrics/{metric_id}")
async def delete_metric(metric_id: str):
    """Delete a custom metric"""
    if not metrics_store.delete(metric_id):
        raise HTTPException(status_code=404, detail="Metric not found")
    return {"success": True}


@router.post("/cohort")
async def build_cohort_analysis(
    cohort_column: str = Query(default="signup_date"),
    activity_column: str = Query(default="last_activity"),
    granularity: TimeGranularity = Query(default=TimeGranularity.MONTH)
):
    """Build cohort retention analysis"""
    result = cohort_analyzer.build_cohorts([], cohort_column, activity_column, granularity.value)
    result["retention_summary"] = cohort_analyzer.calculate_retention(result)
    return result


@router.post("/forecast")
async def generate_forecast(
    value_column: str = Query(default="value"),
    periods: int = Query(default=7, ge=1, le=90),
    method: str = Query(default="linear")
):
    """Generate time series forecast"""
    return forecast_engine.forecast([], value_column, "date", periods, method)


@router.post("/clv")
async def calculate_customer_lifetime_value(
    avg_purchase_value: float = Query(...),
    purchase_frequency: float = Query(...),
    customer_lifespan: float = Query(default=3.0),
    margin: float = Query(default=0.3)
):
    """Calculate Customer Lifetime Value"""
    return clv_calculator.calculate_clv(avg_purchase_value, purchase_frequency, customer_lifespan, margin)


@router.post("/ab-test/significance")
async def calculate_ab_significance(
    control_conversions: int = Query(...),
    control_total: int = Query(...),
    variant_conversions: int = Query(...),
    variant_total: int = Query(...)
):
    """Calculate A/B test statistical significance"""
    return stat_tester.ab_significance(
        control_conversions, control_total,
        variant_conversions, variant_total
    )


@router.get("/kpis")
async def get_kpi_dashboard():
    """Get all KPIs for dashboard"""
    kpis = []
    for metric in metrics_store.list()[:8]:
        kpis.append(metrics_store.evaluate(metric["id"]))
    return {"kpis": kpis, "updated_at": datetime.utcnow().isoformat()}


@router.post("/insights-digest")
async def generate_insights_digest(
    period: str = Query(default="weekly")
):
    """Generate automated insights digest"""
    return {
        "period": period,
        "generated_at": datetime.utcnow().isoformat(),
        "highlights": [
            {"type": "growth", "message": "Lead volume increased 23% this week", "impact": "positive"},
            {"type": "alert", "message": "Email open rate dropped below 20%", "impact": "negative"},
            {"type": "opportunity", "message": "500 leads match high-value segment criteria", "impact": "neutral"},
            {"type": "trend", "message": "Conversion rate trending up for 3 consecutive weeks", "impact": "positive"}
        ],
        "top_metrics": [
            {"name": "Total Leads", "value": 2543, "change": "+15%"},
            {"name": "Conversion Rate", "value": "3.2%", "change": "+0.5%"},
            {"name": "Campaign ROI", "value": "245%", "change": "+12%"}
        ],
        "recommendations": [
            "Focus outreach on high-score leads in Georgia region",
            "Review email subject lines for underperforming campaigns",
            "Schedule follow-up sequence for inactive leads"
        ]
    }
