"""
AI Governance Routes for ACA DataHub
Model cards, bias detection, fairness metrics, explainability, and approval workflows
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from enum import Enum
import random

router = APIRouter(prefix="/ai-governance", tags=["AI Governance"])


# =========================================================================
# Models
# =========================================================================

class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ApprovalStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    REVIEW_REQUIRED = "review_required"


class FairnessMetric(str, Enum):
    DEMOGRAPHIC_PARITY = "demographic_parity"
    EQUAL_OPPORTUNITY = "equal_opportunity"
    EQUALIZED_ODDS = "equalized_odds"
    PREDICTIVE_PARITY = "predictive_parity"


# =========================================================================
# Model Card Manager
# =========================================================================

class ModelCardManager:
    """Manage model documentation and metadata"""
    
    def __init__(self):
        self.model_cards: Dict[str, dict] = {}
        self._counter = 0
    
    def create_model_card(
        self,
        model_id: str,
        model_name: str,
        model_type: str,
        description: str,
        intended_use: str,
        limitations: List[str] = None,
        training_data: dict = None,
        performance_metrics: dict = None,
        ethical_considerations: List[str] = None
    ) -> dict:
        self._counter += 1
        card_id = f"card_{self._counter}"
        
        card = {
            "id": card_id,
            "model_id": model_id,
            "model_name": model_name,
            "model_type": model_type,
            "version": "1.0.0",
            "description": description,
            "intended_use": intended_use,
            "out_of_scope_uses": ["Automated decision-making without human oversight", "Critical safety applications"],
            "limitations": limitations or [],
            "training_data": training_data or {
                "dataset": "ACA DataHub Training Set",
                "size": "1M records",
                "date_range": "2020-2024"
            },
            "performance_metrics": performance_metrics or {},
            "ethical_considerations": ethical_considerations or [],
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "created_by": "data_science_team",
            "status": "draft"
        }
        
        self.model_cards[model_id] = card
        return card
    
    def update_model_card(self, model_id: str, updates: dict) -> dict:
        if model_id not in self.model_cards:
            raise ValueError("Model card not found")
        
        card = self.model_cards[model_id]
        card.update(updates)
        card["updated_at"] = datetime.utcnow().isoformat()
        
        # Increment version for significant updates
        if "performance_metrics" in updates or "training_data" in updates:
            parts = card["version"].split(".")
            parts[-1] = str(int(parts[-1]) + 1)
            card["version"] = ".".join(parts)
        
        return card
    
    def get_model_card(self, model_id: str) -> Optional[dict]:
        return self.model_cards.get(model_id)


model_cards = ModelCardManager()


# =========================================================================
# Bias Detector
# =========================================================================

class BiasDetector:
    """Detect and analyze model bias"""
    
    def __init__(self):
        self.analyses: List[dict] = []
        self._counter = 0
    
    def analyze_bias(
        self,
        model_id: str,
        predictions: List[dict],
        protected_attributes: List[str],
        outcome_field: str
    ) -> dict:
        """Analyze bias in model predictions"""
        self._counter += 1
        analysis_id = f"bias_{self._counter}"
        
        bias_scores = {}
        recommendations = []
        
        for attr in protected_attributes:
            # Group predictions by protected attribute
            groups = {}
            for pred in predictions:
                group_val = pred.get(attr, "unknown")
                if group_val not in groups:
                    groups[group_val] = []
                groups[group_val].append(pred.get(outcome_field, 0))
            
            # Calculate disparities
            if len(groups) >= 2:
                group_rates = {}
                for group, outcomes in groups.items():
                    positive_rate = sum(1 for o in outcomes if o > 0.5) / len(outcomes) if outcomes else 0
                    group_rates[group] = positive_rate
                
                rates = list(group_rates.values())
                if rates:
                    disparity = max(rates) - min(rates)
                    bias_scores[attr] = {
                        "group_rates": group_rates,
                        "disparity": round(disparity, 4),
                        "bias_detected": disparity > 0.1,
                        "severity": "high" if disparity > 0.2 else ("medium" if disparity > 0.1 else "low")
                    }
                    
                    if disparity > 0.1:
                        recommendations.append({
                            "attribute": attr,
                            "recommendation": f"Address disparity in {attr}: consider resampling or fairness constraints",
                            "priority": "high" if disparity > 0.2 else "medium"
                        })
        
        analysis = {
            "id": analysis_id,
            "model_id": model_id,
            "protected_attributes": protected_attributes,
            "outcome_field": outcome_field,
            "sample_size": len(predictions),
            "bias_scores": bias_scores,
            "overall_bias_detected": any(s.get("bias_detected") for s in bias_scores.values()),
            "recommendations": recommendations,
            "analyzed_at": datetime.utcnow().isoformat()
        }
        
        self.analyses.append(analysis)
        return analysis
    
    def get_fairness_metrics(
        self,
        model_id: str,
        metrics: List[str] = None
    ) -> dict:
        """Calculate fairness metrics"""
        metrics = metrics or ["demographic_parity", "equal_opportunity"]
        
        results = {}
        for metric in metrics:
            # Simulate fairness metric calculation
            score = round(random.uniform(0.7, 1.0), 3)
            threshold = 0.8
            
            results[metric] = {
                "score": score,
                "threshold": threshold,
                "passed": score >= threshold,
                "description": self._get_metric_description(metric)
            }
        
        return {
            "model_id": model_id,
            "metrics": results,
            "overall_fair": all(m["passed"] for m in results.values()),
            "calculated_at": datetime.utcnow().isoformat()
        }
    
    def _get_metric_description(self, metric: str) -> str:
        descriptions = {
            "demographic_parity": "Equal positive prediction rates across groups",
            "equal_opportunity": "Equal true positive rates across groups",
            "equalized_odds": "Equal TPR and FPR across groups",
            "predictive_parity": "Equal precision across groups"
        }
        return descriptions.get(metric, "Unknown metric")


bias_detector = BiasDetector()


# =========================================================================
# Explainability Engine
# =========================================================================

class ExplainabilityEngine:
    """Generate model explanations"""
    
    def __init__(self):
        self.explanations: List[dict] = []
        self._counter = 0
    
    def explain_prediction(
        self,
        model_id: str,
        input_data: dict,
        prediction: float
    ) -> dict:
        """Generate explanation for a single prediction"""
        self._counter += 1
        
        # Simulate SHAP-like feature contributions
        features = list(input_data.keys())
        total_contribution = prediction - 0.5  # Difference from base
        
        contributions = []
        remaining = total_contribution
        
        for i, feature in enumerate(features[:-1]):
            # Assign random contribution
            contrib = round(random.uniform(-0.2, 0.2), 4)
            remaining -= contrib
            contributions.append({
                "feature": feature,
                "value": input_data[feature],
                "contribution": contrib,
                "direction": "positive" if contrib > 0 else "negative"
            })
        
        # Last feature gets remaining contribution
        if features:
            contributions.append({
                "feature": features[-1],
                "value": input_data[features[-1]],
                "contribution": round(remaining, 4),
                "direction": "positive" if remaining > 0 else "negative"
            })
        
        # Sort by absolute contribution
        contributions.sort(key=lambda x: abs(x["contribution"]), reverse=True)
        
        explanation = {
            "id": f"exp_{self._counter}",
            "model_id": model_id,
            "prediction": prediction,
            "base_value": 0.5,
            "contributions": contributions[:10],
            "top_features": [c["feature"] for c in contributions[:3]],
            "explanation_summary": self._generate_summary(contributions[:3], prediction),
            "generated_at": datetime.utcnow().isoformat()
        }
        
        self.explanations.append(explanation)
        return explanation
    
    def _generate_summary(self, top_contributions: List[dict], prediction: float) -> str:
        if not top_contributions:
            return "Unable to generate summary"
        
        direction = "high" if prediction > 0.5 else "low"
        top_feature = top_contributions[0]
        
        return f"The prediction is {direction} ({prediction:.2f}) primarily due to {top_feature['feature']} = {top_feature['value']}"
    
    def generate_report(self, model_id: str, sample_size: int = 100) -> dict:
        """Generate explainability report"""
        model_explanations = [e for e in self.explanations if e["model_id"] == model_id]
        
        # Aggregate feature importance
        feature_importance = {}
        for exp in model_explanations[-sample_size:]:
            for contrib in exp.get("contributions", []):
                feature = contrib["feature"]
                if feature not in feature_importance:
                    feature_importance[feature] = []
                feature_importance[feature].append(abs(contrib["contribution"]))
        
        importance_summary = []
        for feature, values in feature_importance.items():
            importance_summary.append({
                "feature": feature,
                "mean_importance": round(sum(values) / len(values), 4),
                "frequency": len(values)
            })
        
        importance_summary.sort(key=lambda x: x["mean_importance"], reverse=True)
        
        return {
            "model_id": model_id,
            "explanations_analyzed": len(model_explanations),
            "feature_importance": importance_summary[:10],
            "generated_at": datetime.utcnow().isoformat()
        }


explainer = ExplainabilityEngine()


# =========================================================================
# Approval Workflow
# =========================================================================

class ApprovalWorkflow:
    """Manage model approval processes"""
    
    def __init__(self):
        self.requests: Dict[str, dict] = {}
        self.audit_log: List[dict] = []
        self._counter = 0
    
    def submit_for_approval(
        self,
        model_id: str,
        model_version: str,
        submitted_by: str,
        deployment_target: str,
        risk_assessment: dict = None
    ) -> dict:
        self._counter += 1
        request_id = f"approval_{self._counter}"
        
        # Auto-calculate risk level
        risk_level = self._assess_risk(risk_assessment or {})
        
        request = {
            "id": request_id,
            "model_id": model_id,
            "model_version": model_version,
            "submitted_by": submitted_by,
            "deployment_target": deployment_target,
            "risk_assessment": risk_assessment or {},
            "risk_level": risk_level,
            "status": ApprovalStatus.PENDING.value,
            "submitted_at": datetime.utcnow().isoformat(),
            "required_approvers": self._get_required_approvers(risk_level),
            "approvals": [],
            "comments": []
        }
        
        self.requests[request_id] = request
        self._log_action(request_id, "submitted", submitted_by)
        
        return request
    
    def _assess_risk(self, assessment: dict) -> str:
        """Assess risk level based on criteria"""
        score = 0
        
        if assessment.get("uses_pii"):
            score += 3
        if assessment.get("automated_decisions"):
            score += 3
        if assessment.get("financial_impact"):
            score += 2
        if assessment.get("customer_facing"):
            score += 2
        
        if score >= 6:
            return RiskLevel.CRITICAL.value
        elif score >= 4:
            return RiskLevel.HIGH.value
        elif score >= 2:
            return RiskLevel.MEDIUM.value
        return RiskLevel.LOW.value
    
    def _get_required_approvers(self, risk_level: str) -> List[str]:
        """Determine required approvers based on risk"""
        if risk_level == RiskLevel.CRITICAL.value:
            return ["ml_lead", "ethics_committee", "cto"]
        elif risk_level == RiskLevel.HIGH.value:
            return ["ml_lead", "ethics_committee"]
        elif risk_level == RiskLevel.MEDIUM.value:
            return ["ml_lead"]
        return ["ml_team"]
    
    def approve(self, request_id: str, approver: str, comments: str = None) -> dict:
        if request_id not in self.requests:
            raise ValueError("Request not found")
        
        request = self.requests[request_id]
        
        request["approvals"].append({
            "approver": approver,
            "action": "approved",
            "comments": comments,
            "timestamp": datetime.utcnow().isoformat()
        })
        
        # Check if all required approvers have approved
        approved_by = set(a["approver"] for a in request["approvals"] if a["action"] == "approved")
        required = set(request["required_approvers"])
        
        if required.issubset(approved_by):
            request["status"] = ApprovalStatus.APPROVED.value
            request["approved_at"] = datetime.utcnow().isoformat()
        
        self._log_action(request_id, "approved", approver, comments)
        return request
    
    def reject(self, request_id: str, rejector: str, reason: str) -> dict:
        if request_id not in self.requests:
            raise ValueError("Request not found")
        
        request = self.requests[request_id]
        request["status"] = ApprovalStatus.REJECTED.value
        request["rejected_at"] = datetime.utcnow().isoformat()
        request["rejection_reason"] = reason
        
        request["approvals"].append({
            "approver": rejector,
            "action": "rejected",
            "comments": reason,
            "timestamp": datetime.utcnow().isoformat()
        })
        
        self._log_action(request_id, "rejected", rejector, reason)
        return request
    
    def _log_action(self, request_id: str, action: str, actor: str, details: str = None):
        self.audit_log.append({
            "request_id": request_id,
            "action": action,
            "actor": actor,
            "details": details,
            "timestamp": datetime.utcnow().isoformat()
        })


approval_workflow = ApprovalWorkflow()


# =========================================================================
# Endpoints
# =========================================================================

@router.post("/model-cards")
async def create_model_card(
    model_id: str = Query(...),
    model_name: str = Query(...),
    model_type: str = Query(...),
    description: str = Query(...),
    intended_use: str = Query(...)
):
    """Create model card"""
    card = model_cards.create_model_card(
        model_id, model_name, model_type, description, intended_use
    )
    return {"success": True, "model_card": card}


@router.get("/model-cards/{model_id}")
async def get_model_card(model_id: str):
    """Get model card"""
    card = model_cards.get_model_card(model_id)
    if not card:
        raise HTTPException(status_code=404, detail="Model card not found")
    return card


@router.put("/model-cards/{model_id}")
async def update_model_card(model_id: str, updates: dict):
    """Update model card"""
    try:
        return model_cards.update_model_card(model_id, updates)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/model-cards")
async def list_model_cards():
    """List all model cards"""
    return {"model_cards": list(model_cards.model_cards.values())}


@router.post("/bias/analyze")
async def analyze_bias(
    model_id: str = Query(...),
    predictions: List[dict] = None,
    protected_attributes: List[str] = Query(...),
    outcome_field: str = Query(...)
):
    """Analyze bias in model predictions"""
    predictions = predictions or []
    return bias_detector.analyze_bias(model_id, predictions, protected_attributes, outcome_field)


@router.get("/bias/analyses/{model_id}")
async def get_bias_analyses(model_id: str):
    """Get bias analyses for a model"""
    analyses = [a for a in bias_detector.analyses if a["model_id"] == model_id]
    return {"analyses": analyses}


@router.get("/fairness/{model_id}")
async def get_fairness_metrics(
    model_id: str,
    metrics: List[FairnessMetric] = Query(default=None)
):
    """Get fairness metrics for a model"""
    metric_names = [m.value for m in metrics] if metrics else None
    return bias_detector.get_fairness_metrics(model_id, metric_names)


@router.post("/explain")
async def explain_prediction(
    model_id: str = Query(...),
    input_data: dict = None,
    prediction: float = Query(...)
):
    """Explain a prediction"""
    return explainer.explain_prediction(model_id, input_data or {}, prediction)


@router.get("/explain/report/{model_id}")
async def get_explainability_report(model_id: str):
    """Get explainability report for a model"""
    return explainer.generate_report(model_id)


@router.post("/approval/submit")
async def submit_for_approval(
    model_id: str = Query(...),
    model_version: str = Query(...),
    submitted_by: str = Query(...),
    deployment_target: str = Query(...),
    risk_assessment: dict = None
):
    """Submit model for approval"""
    return approval_workflow.submit_for_approval(
        model_id, model_version, submitted_by, deployment_target, risk_assessment
    )


@router.get("/approval/requests")
async def list_approval_requests(status: Optional[str] = Query(default=None)):
    """List approval requests"""
    requests = list(approval_workflow.requests.values())
    if status:
        requests = [r for r in requests if r["status"] == status]
    return {"requests": requests}


@router.get("/approval/requests/{request_id}")
async def get_approval_request(request_id: str):
    """Get approval request details"""
    request = approval_workflow.requests.get(request_id)
    if not request:
        raise HTTPException(status_code=404, detail="Request not found")
    return request


@router.post("/approval/requests/{request_id}/approve")
async def approve_request(
    request_id: str,
    approver: str = Query(...),
    comments: str = Query(default=None)
):
    """Approve a request"""
    try:
        return approval_workflow.approve(request_id, approver, comments)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/approval/requests/{request_id}/reject")
async def reject_request(
    request_id: str,
    rejector: str = Query(...),
    reason: str = Query(...)
):
    """Reject a request"""
    try:
        return approval_workflow.reject(request_id, rejector, reason)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/audit-log")
async def get_audit_log(limit: int = Query(default=100)):
    """Get AI governance audit log"""
    return {"log": approval_workflow.audit_log[-limit:]}
