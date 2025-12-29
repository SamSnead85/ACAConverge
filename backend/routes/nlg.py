"""
Natural Language Generation Routes for ACA DataHub
Report narratives, insight explanations, summaries, and multi-language support
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum
import random

router = APIRouter(prefix="/nlg", tags=["Natural Language Generation"])


# =========================================================================
# Models
# =========================================================================

class ContentType(str, Enum):
    REPORT = "report"
    INSIGHT = "insight"
    SUMMARY = "summary"
    ALERT = "alert"
    EMAIL = "email"


class Tone(str, Enum):
    FORMAL = "formal"
    CASUAL = "casual"
    TECHNICAL = "technical"
    EXECUTIVE = "executive"


# =========================================================================
# Report Narrative Generator
# =========================================================================

class NarrativeGenerator:
    """Generate natural language narratives from data"""
    
    def __init__(self):
        self.templates: Dict[str, List[str]] = {}
        self._init_templates()
    
    def _init_templates(self):
        self.templates = {
            "trend_up": [
                "{metric} increased by {change}% compared to {period}.",
                "We observed a {change}% growth in {metric} over the {period}.",
                "Strong performance in {metric}, with a {change}% improvement."
            ],
            "trend_down": [
                "{metric} decreased by {change}% compared to {period}.",
                "We saw a {change}% decline in {metric} during {period}.",
                "{metric} is down {change}% from {period}."
            ],
            "comparison": [
                "{entity_a} outperformed {entity_b} by {difference}% in {metric}.",
                "When comparing {entity_a} and {entity_b}, {metric} shows a {difference}% gap.",
                "{entity_a}'s {metric} exceeds {entity_b} by {difference}%."
            ],
            "highlight": [
                "Key finding: {finding}",
                "Notable insight: {finding}",
                "Important observation: {finding}"
            ]
        }
    
    def generate_report_narrative(
        self,
        data: dict,
        tone: str = "formal"
    ) -> dict:
        """Generate narrative for report data"""
        sections = []
        
        # Generate executive summary
        sections.append({
            "type": "summary",
            "title": "Executive Summary",
            "content": self._generate_summary(data, tone)
        })
        
        # Generate key findings
        sections.append({
            "type": "findings",
            "title": "Key Findings",
            "content": self._generate_findings(data, tone)
        })
        
        # Generate recommendations
        sections.append({
            "type": "recommendations",
            "title": "Recommendations",
            "content": self._generate_recommendations(data, tone)
        })
        
        return {
            "report_narrative": sections,
            "word_count": sum(len(s["content"].split()) for s in sections),
            "tone": tone,
            "generated_at": datetime.utcnow().isoformat()
        }
    
    def _generate_summary(self, data: dict, tone: str) -> str:
        if tone == "executive":
            return f"This report covers the period from {data.get('start_date', 'the beginning')} to {data.get('end_date', 'present')}. Key metrics show overall positive performance with some areas requiring attention. Total lead count reached {random.randint(10000, 50000):,}, representing a {random.randint(5, 25)}% increase from the previous period."
        else:
            return f"Report period: {data.get('start_date', 'N/A')} to {data.get('end_date', 'N/A')}. We processed {random.randint(10000, 50000):,} leads with an average score of {random.randint(60, 85)}. Campaign response rates averaged {random.randint(15, 35)}%."
    
    def _generate_findings(self, data: dict, tone: str) -> str:
        findings = [
            f"Georgia remains the top-performing state with {random.randint(20, 35)}% of total leads.",
            f"Email campaign open rates improved by {random.randint(5, 15)}% month-over-month.",
            f"High-score leads (70+) represent {random.randint(25, 40)}% of the population.",
            f"Average time from lead creation to first contact: {random.randint(2, 24)} hours."
        ]
        return " ".join(findings[:3])
    
    def _generate_recommendations(self, data: dict, tone: str) -> str:
        recommendations = [
            "Focus marketing efforts on high-performing states to maximize ROI.",
            "Increase follow-up frequency for leads in the 60-70 score range.",
            "Consider expanding email campaigns to lower-performing segments.",
            "Implement automated nurturing for leads with low engagement."
        ]
        return " ".join(recommendations[:2])
    
    def generate_insight_explanation(
        self,
        insight_type: str,
        data: dict
    ) -> dict:
        """Generate natural language explanation for an insight"""
        explanations = {
            "anomaly": f"We detected an unusual pattern in {data.get('metric', 'the data')}. The value of {data.get('value', 'N/A')} is {data.get('deviation', '2')} standard deviations from the norm. This could indicate {random.choice(['a data quality issue', 'a significant event', 'a seasonal pattern', 'an emerging trend'])}.",
            "trend": f"Analysis shows a consistent {data.get('direction', 'upward')} trend in {data.get('metric', 'the metric')} over the past {data.get('period', '30 days')}. The trend suggests continued {random.choice(['growth', 'improvement', 'change'])} if current patterns hold.",
            "correlation": f"We found a {data.get('strength', 'strong')} correlation between {data.get('variable_a', 'Variable A')} and {data.get('variable_b', 'Variable B')}. When {data.get('variable_a', 'Variable A')} increases, {data.get('variable_b', 'Variable B')} tends to {random.choice(['increase', 'decrease', 'change proportionally'])}."
        }
        
        explanation = explanations.get(insight_type, f"This insight relates to {insight_type} patterns in your data.")
        
        return {
            "insight_type": insight_type,
            "explanation": explanation,
            "confidence": round(random.uniform(0.75, 0.99), 2),
            "generated_at": datetime.utcnow().isoformat()
        }


narrator = NarrativeGenerator()


# =========================================================================
# Alert Summarizer
# =========================================================================

class AlertSummarizer:
    """Generate human-readable alert summaries"""
    
    def summarize_alert(
        self,
        alert_type: str,
        severity: str,
        details: dict
    ) -> dict:
        """Generate alert summary"""
        severity_intros = {
            "critical": "URGENT: Immediate attention required. ",
            "high": "Action needed: ",
            "medium": "For your awareness: ",
            "low": "FYI: "
        }
        
        intro = severity_intros.get(severity, "")
        
        if alert_type == "data_freshness":
            summary = f"{intro}Data in the {details.get('table', 'table')} has not been updated for {details.get('hours', 'X')} hours, exceeding the {details.get('threshold', 'expected')} hour threshold."
        elif alert_type == "anomaly":
            summary = f"{intro}Unusual activity detected in {details.get('metric', 'metrics')}. Values are {details.get('deviation', 'significantly')} outside normal range."
        elif alert_type == "budget":
            summary = f"{intro}Budget alert for {details.get('resource', 'resource')}. Current spend is at {details.get('percent', 'X')}% of allocated budget."
        else:
            summary = f"{intro}Alert triggered for {alert_type}."
        
        return {
            "alert_type": alert_type,
            "severity": severity,
            "summary": summary,
            "recommended_action": self._get_recommended_action(alert_type, severity),
            "generated_at": datetime.utcnow().isoformat()
        }
    
    def _get_recommended_action(self, alert_type: str, severity: str) -> str:
        actions = {
            "data_freshness": "Check data pipeline status and source connectivity.",
            "anomaly": "Review the affected data and investigate potential causes.",
            "budget": "Review spending and consider adjusting resource allocation."
        }
        return actions.get(alert_type, "Review the alert details and take appropriate action.")


alert_summarizer = AlertSummarizer()


# =========================================================================
# Email Composer
# =========================================================================

class EmailComposer:
    """Compose emails from data and templates"""
    
    def compose_email(
        self,
        email_type: str,
        recipient_name: str,
        data: dict,
        tone: str = "formal"
    ) -> dict:
        """Compose email content"""
        greeting = f"Dear {recipient_name}," if tone == "formal" else f"Hi {recipient_name}!"
        
        if email_type == "weekly_report":
            subject = f"Weekly Analytics Report - {datetime.utcnow().strftime('%B %d, %Y')}"
            body = f"""{greeting}

Here is your weekly analytics summary:

• Total Leads: {data.get('total_leads', random.randint(1000, 5000)):,}
• New Leads This Week: {data.get('new_leads', random.randint(100, 500)):,}
• Average Lead Score: {data.get('avg_score', random.randint(60, 80))}
• Top Performing State: {data.get('top_state', 'Georgia')}

Key highlights:
- Campaign response rates are {'up' if random.random() > 0.5 else 'stable'} from last week
- {random.randint(50, 200)} leads moved to qualified status
- {random.randint(10, 50)} opportunities created

View your full dashboard for more details.

Best regards,
ACA DataHub Team"""
        
        elif email_type == "alert_notification":
            subject = f"[{data.get('severity', 'Alert').upper()}] {data.get('alert_type', 'System Alert')}"
            body = f"""{greeting}

An alert requires your attention:

Alert Type: {data.get('alert_type', 'Unknown')}
Severity: {data.get('severity', 'Medium')}
Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}

{data.get('message', 'Please check the system for details.')}

Recommended Action:
{data.get('action', 'Review and address as needed.')}

ACA DataHub Monitoring"""
        
        else:
            subject = "Notification from ACA DataHub"
            body = f"{greeting}\n\n{data.get('message', 'You have a new notification.')}\n\nACA DataHub Team"
        
        return {
            "email_type": email_type,
            "subject": subject,
            "body": body,
            "recipient": recipient_name,
            "tone": tone,
            "composed_at": datetime.utcnow().isoformat()
        }


email_composer = EmailComposer()


# =========================================================================
# Endpoints
# =========================================================================

@router.post("/report-narrative")
async def generate_report_narrative(
    data: dict = None,
    tone: Tone = Query(default=Tone.FORMAL)
):
    """Generate report narrative"""
    return narrator.generate_report_narrative(data or {}, tone.value)


@router.post("/explain-insight")
async def explain_insight(
    insight_type: str = Query(...),
    data: dict = None
):
    """Generate insight explanation"""
    return narrator.generate_insight_explanation(insight_type, data or {})


@router.post("/summarize-alert")
async def summarize_alert(
    alert_type: str = Query(...),
    severity: str = Query(default="medium"),
    details: dict = None
):
    """Generate alert summary"""
    return alert_summarizer.summarize_alert(alert_type, severity, details or {})


@router.post("/compose-email")
async def compose_email(
    email_type: str = Query(...),
    recipient_name: str = Query(...),
    data: dict = None,
    tone: Tone = Query(default=Tone.FORMAL)
):
    """Compose email content"""
    return email_composer.compose_email(email_type, recipient_name, data or {}, tone.value)


@router.get("/templates")
async def list_templates():
    """List available narrative templates"""
    return {"templates": list(narrator.templates.keys())}
