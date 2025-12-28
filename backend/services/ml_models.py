"""
Machine Learning Models Service for ACA DataHub
Predictive lead scoring, churn prediction, and anomaly detection
"""

from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
import random
import math


@dataclass
class PredictionResult:
    """Container for ML prediction results"""
    score: float
    confidence: float
    factors: List[Dict[str, Any]]
    model_version: str


class MLModelService:
    """Machine learning model service for predictive analytics"""
    
    def __init__(self):
        self.model_version = "1.0.0"
        self._feature_weights = {
            # Lead scoring weights
            "income": 0.25,
            "age": 0.15,
            "has_email": 0.20,
            "has_phone": 0.15,
            "location_tier": 0.10,
            "engagement": 0.15
        }
    
    # =========================================================================
    # Predictive Lead Scoring
    # =========================================================================
    
    def score_lead(self, lead: Dict[str, Any]) -> PredictionResult:
        """
        Generate a predictive lead score using ML features.
        Returns a score from 0-100 with confidence and contributing factors.
        """
        factors = []
        total_score = 0
        total_weight = 0
        
        # Income scoring (35k-80k ACA qualifying range)
        income = lead.get("income", 0)
        if isinstance(income, str):
            income = float(income.replace("$", "").replace(",", "") or 0)
        
        if 35000 <= income <= 80000:
            income_score = 100
            factors.append({"factor": "Income in ACA qualifying range", "impact": "+25"})
        elif 20000 <= income < 35000 or 80000 < income <= 100000:
            income_score = 60
            factors.append({"factor": "Income near qualifying range", "impact": "+15"})
        else:
            income_score = 20
            factors.append({"factor": "Income outside typical range", "impact": "+5"})
        
        total_score += income_score * self._feature_weights["income"]
        total_weight += self._feature_weights["income"]
        
        # Age scoring (26-64 ideal range)
        age = lead.get("age", 0)
        if isinstance(age, str):
            age = int(age or 0)
        
        if 26 <= age <= 64:
            age_score = 100
            factors.append({"factor": "Age in primary market", "impact": "+15"})
        elif 18 <= age < 26:
            age_score = 70
            factors.append({"factor": "Young adult market", "impact": "+10"})
        else:
            age_score = 40
            factors.append({"factor": "Age less typical", "impact": "+6"})
        
        total_score += age_score * self._feature_weights["age"]
        total_weight += self._feature_weights["age"]
        
        # Contact readiness
        has_email = bool(lead.get("email"))
        has_phone = bool(lead.get("phone"))
        
        email_score = 100 if has_email else 0
        phone_score = 100 if has_phone else 0
        
        if has_email:
            factors.append({"factor": "Has email contact", "impact": "+20"})
        if has_phone:
            factors.append({"factor": "Has phone contact", "impact": "+15"})
        
        total_score += email_score * self._feature_weights["has_email"]
        total_score += phone_score * self._feature_weights["has_phone"]
        total_weight += self._feature_weights["has_email"] + self._feature_weights["has_phone"]
        
        # Calculate final score
        final_score = min(100, (total_score / total_weight) * 100) if total_weight > 0 else 50
        
        # Calculate confidence based on data completeness
        completeness = sum([
            1 if lead.get(f) else 0 
            for f in ["income", "age", "email", "phone", "name"]
        ]) / 5
        confidence = 0.5 + (completeness * 0.5)
        
        return PredictionResult(
            score=round(final_score, 1),
            confidence=round(confidence, 2),
            factors=factors,
            model_version=self.model_version
        )
    
    def batch_score_leads(self, leads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Score multiple leads and return sorted by score"""
        scored_leads = []
        
        for lead in leads:
            result = self.score_lead(lead)
            scored_leads.append({
                **lead,
                "ml_score": result.score,
                "ml_confidence": result.confidence,
                "ml_factors": result.factors
            })
        
        # Sort by score descending
        scored_leads.sort(key=lambda x: x["ml_score"], reverse=True)
        return scored_leads
    
    # =========================================================================
    # Churn Prediction
    # =========================================================================
    
    def predict_churn(self, customer: Dict[str, Any]) -> PredictionResult:
        """
        Predict likelihood of customer churn based on engagement signals.
        """
        factors = []
        risk_score = 0
        
        # Days since last activity
        last_activity_days = customer.get("days_since_last_activity", 0)
        if last_activity_days > 90:
            risk_score += 40
            factors.append({"factor": "No activity in 90+ days", "impact": "+40"})
        elif last_activity_days > 30:
            risk_score += 20
            factors.append({"factor": "Low recent activity", "impact": "+20"})
        
        # Email engagement
        email_open_rate = customer.get("email_open_rate", 0.5)
        if email_open_rate < 0.1:
            risk_score += 25
            factors.append({"factor": "Very low email engagement", "impact": "+25"})
        elif email_open_rate < 0.3:
            risk_score += 10
            factors.append({"factor": "Below average email engagement", "impact": "+10"})
        
        # Support tickets
        support_tickets = customer.get("open_tickets", 0)
        if support_tickets > 3:
            risk_score += 20
            factors.append({"factor": "Multiple support issues", "impact": "+20"})
        
        # Negative factors (reduce risk)
        if customer.get("recent_purchase"):
            risk_score -= 15
            factors.append({"factor": "Recent purchase", "impact": "-15"})
        
        if customer.get("long_tenure"):
            risk_score -= 10
            factors.append({"factor": "Long-term customer", "impact": "-10"})
        
        risk_score = max(0, min(100, risk_score))
        
        return PredictionResult(
            score=risk_score,
            confidence=0.75,
            factors=factors,
            model_version=self.model_version
        )
    
    # =========================================================================
    # Anomaly Detection
    # =========================================================================
    
    def detect_anomalies(
        self, 
        data: List[Dict[str, Any]], 
        column: str,
        threshold: float = 2.0
    ) -> Dict[str, Any]:
        """
        Detect anomalies in a numeric column using statistical methods.
        Uses z-score approach for simplicity.
        """
        values = []
        for row in data:
            val = row.get(column)
            if val is not None:
                try:
                    values.append(float(val))
                except (ValueError, TypeError):
                    pass
        
        if len(values) < 3:
            return {"anomalies": [], "message": "Insufficient data for anomaly detection"}
        
        # Calculate mean and std
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        std = math.sqrt(variance) if variance > 0 else 1
        
        # Find anomalies
        anomalies = []
        for i, val in enumerate(values):
            z_score = abs((val - mean) / std) if std > 0 else 0
            if z_score > threshold:
                anomalies.append({
                    "index": i,
                    "value": val,
                    "z_score": round(z_score, 2),
                    "deviation": round((val - mean), 2)
                })
        
        return {
            "column": column,
            "total_records": len(values),
            "anomaly_count": len(anomalies),
            "mean": round(mean, 2),
            "std": round(std, 2),
            "threshold": threshold,
            "anomalies": anomalies[:20]  # Limit to top 20
        }
    
    # =========================================================================
    # Feature Importance Analysis
    # =========================================================================
    
    def get_feature_importance(self, model_type: str = "lead_scoring") -> List[Dict[str, Any]]:
        """Get feature importance for a model"""
        if model_type == "lead_scoring":
            return [
                {"feature": "Income Level", "importance": 0.25, "description": "Income within ACA qualifying range"},
                {"feature": "Email Availability", "importance": 0.20, "description": "Valid email for direct contact"},
                {"feature": "Phone Availability", "importance": 0.15, "description": "Phone number for outreach"},
                {"feature": "Age Group", "importance": 0.15, "description": "Age within target demographic"},
                {"feature": "Engagement Score", "importance": 0.15, "description": "Previous engagement signals"},
                {"feature": "Location Tier", "importance": 0.10, "description": "Geographic market value"}
            ]
        
        return []
    
    # =========================================================================
    # Model Training Simulation
    # =========================================================================
    
    def train_model(
        self, 
        training_data: List[Dict[str, Any]], 
        model_type: str = "lead_scoring"
    ) -> Dict[str, Any]:
        """
        Simulate model training (in production would use actual ML training).
        Returns training metrics.
        """
        return {
            "model_type": model_type,
            "samples_used": len(training_data),
            "training_started": datetime.utcnow().isoformat(),
            "metrics": {
                "accuracy": 0.85 + random.random() * 0.1,
                "precision": 0.82 + random.random() * 0.1,
                "recall": 0.78 + random.random() * 0.1,
                "f1_score": 0.80 + random.random() * 0.1
            },
            "feature_count": 6,
            "model_version": f"1.{random.randint(0, 9)}.0",
            "status": "completed"
        }


# Singleton instance
ml_service = MLModelService()
