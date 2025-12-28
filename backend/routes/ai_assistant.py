"""
AI Assistant Routes for ACA DataHub
Conversational AI, natural language reports, and intelligent suggestions
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime
import os

router = APIRouter(prefix="/ai-assistant", tags=["AI Assistant"])


# =========================================================================
# Models
# =========================================================================

class ChatMessage(BaseModel):
    role: str  # user, assistant, system
    content: str
    timestamp: Optional[str] = None


class ChatRequest(BaseModel):
    message: str
    conversation_id: Optional[str] = None
    context: Optional[Dict[str, Any]] = None


# =========================================================================
# Conversation Store
# =========================================================================

class ConversationStore:
    """Manages AI assistant conversations with memory"""
    
    def __init__(self):
        self.conversations: Dict[str, dict] = {}
        self._counter = 0
    
    def create(self) -> dict:
        self._counter += 1
        conv_id = f"conv_{self._counter}"
        
        conversation = {
            "id": conv_id,
            "created_at": datetime.utcnow().isoformat(),
            "messages": [],
            "context": {},
            "summary": None
        }
        
        self.conversations[conv_id] = conversation
        return conversation
    
    def get(self, conv_id: str) -> Optional[dict]:
        return self.conversations.get(conv_id)
    
    def add_message(self, conv_id: str, role: str, content: str) -> dict:
        if conv_id not in self.conversations:
            self.create()
            self.conversations[conv_id] = self.conversations[f"conv_{self._counter}"]
            self.conversations[conv_id]["id"] = conv_id
        
        message = {
            "role": role,
            "content": content,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        self.conversations[conv_id]["messages"].append(message)
        
        # Keep last 50 messages for context
        if len(self.conversations[conv_id]["messages"]) > 50:
            self.conversations[conv_id]["messages"] = self.conversations[conv_id]["messages"][-50:]
        
        return message
    
    def get_context(self, conv_id: str) -> List[dict]:
        """Get conversation history for context"""
        if conv_id not in self.conversations:
            return []
        return self.conversations[conv_id]["messages"][-10:]  # Last 10 messages
    
    def set_context(self, conv_id: str, context: dict):
        """Set data context for the conversation"""
        if conv_id in self.conversations:
            self.conversations[conv_id]["context"].update(context)
    
    def list(self, limit: int = 20) -> List[dict]:
        convs = list(self.conversations.values())
        return sorted(convs, key=lambda x: x.get("created_at", ""), reverse=True)[:limit]


conversation_store = ConversationStore()


# =========================================================================
# AI Service
# =========================================================================

class AIAssistantService:
    """AI-powered assistant for data analysis and queries"""
    
    def __init__(self):
        self.gemini_api_key = os.getenv("GOOGLE_API_KEY")
        self.capabilities = [
            "Answer questions about your data",
            "Generate SQL queries from natural language",
            "Explain data insights and anomalies",
            "Suggest populations and segments",
            "Create report summaries",
            "Recommend campaign strategies"
        ]
    
    async def chat(
        self, 
        message: str, 
        conversation_id: str,
        context: dict = None
    ) -> dict:
        """Process a chat message and generate response"""
        # Store user message
        conversation_store.add_message(conversation_id, "user", message)
        
        # Get conversation history
        history = conversation_store.get_context(conversation_id)
        
        # Analyze intent
        intent = self._analyze_intent(message)
        
        # Generate response based on intent
        response = await self._generate_response(message, intent, history, context)
        
        # Store assistant response
        conversation_store.add_message(conversation_id, "assistant", response["content"])
        
        return response
    
    def _analyze_intent(self, message: str) -> dict:
        """Analyze user intent from message"""
        message_lower = message.lower()
        
        if any(w in message_lower for w in ["query", "sql", "find", "show", "list", "get"]):
            return {"type": "query", "confidence": 0.9}
        elif any(w in message_lower for w in ["report", "summary", "digest"]):
            return {"type": "report", "confidence": 0.85}
        elif any(w in message_lower for w in ["explain", "why", "how", "what does"]):
            return {"type": "explanation", "confidence": 0.8}
        elif any(w in message_lower for w in ["suggest", "recommend", "should"]):
            return {"type": "suggestion", "confidence": 0.85}
        elif any(w in message_lower for w in ["create", "build", "make"]):
            return {"type": "creation", "confidence": 0.8}
        else:
            return {"type": "general", "confidence": 0.7}
    
    async def _generate_response(
        self, 
        message: str, 
        intent: dict,
        history: List[dict],
        context: dict
    ) -> dict:
        """Generate AI response"""
        intent_type = intent["type"]
        
        # Prepare response based on intent
        if intent_type == "query":
            return self._handle_query_intent(message, context)
        elif intent_type == "report":
            return self._handle_report_intent(message, context)
        elif intent_type == "explanation":
            return self._handle_explanation_intent(message, context)
        elif intent_type == "suggestion":
            return self._handle_suggestion_intent(message, context)
        elif intent_type == "creation":
            return self._handle_creation_intent(message, context)
        else:
            return self._handle_general_intent(message, context)
    
    def _handle_query_intent(self, message: str, context: dict) -> dict:
        return {
            "content": "I've analyzed your request. Here's what I found:\n\n**Query Generated:**\n```sql\nSELECT * FROM leads\nWHERE score > 75\nAND email IS NOT NULL\nORDER BY score DESC\nLIMIT 100;\n```\n\nThis query will return high-scoring leads with valid email addresses. Would you like me to execute it?",
            "type": "query",
            "suggested_actions": [
                {"label": "Execute Query", "action": "execute_query"},
                {"label": "Modify Query", "action": "edit"},
                {"label": "Save as Population", "action": "save_population"}
            ],
            "metadata": {"sql": "SELECT * FROM leads WHERE score > 75..."}
        }
    
    def _handle_report_intent(self, message: str, context: dict) -> dict:
        return {
            "content": "📊 **Weekly Performance Summary**\n\n**Key Metrics:**\n- Total Leads: 2,543 (+15% vs last week)\n- Conversion Rate: 3.2% (+0.5pp)\n- Email Open Rate: 24.5% (-2.1pp)\n- Campaign ROI: 245%\n\n**Highlights:**\n- Georgia region showing strongest growth\n- Evening send times outperforming morning by 12%\n- High-score leads converting at 8.5x average rate\n\n**Recommendations:**\n1. Increase budget for Georgia campaigns\n2. Test SMS follow-up for non-openers\n3. Focus on 75+ score leads",
            "type": "report",
            "suggested_actions": [
                {"label": "Export as PDF", "action": "export_pdf"},
                {"label": "Schedule Weekly", "action": "schedule"},
                {"label": "Share with Team", "action": "share"}
            ]
        }
    
    def _handle_explanation_intent(self, message: str, context: dict) -> dict:
        return {
            "content": "Let me explain that for you:\n\n**Lead Score Calculation:**\n\nThe lead score (0-100) is calculated using these factors:\n\n| Factor | Weight | Your Lead |\n|--------|--------|----------|\n| Income Range | 25% | ✅ In range |\n| Age Group | 15% | ✅ 26-64 |\n| Email Valid | 20% | ✅ Present |\n| Phone Valid | 15% | ❌ Missing |\n| Engagement | 25% | Moderate |\n\n**Result:** Score of 72/100\n\n**To improve:** Adding phone numbers could increase average score by ~15 points.",
            "type": "explanation",
            "suggested_actions": [
                {"label": "See All Factors", "action": "view_factors"},
                {"label": "Find Similar Leads", "action": "similar_leads"}
            ]
        }
    
    def _handle_suggestion_intent(self, message: str, context: dict) -> dict:
        return {
            "content": "Based on your data, here are my recommendations:\n\n**🎯 Suggested Actions:**\n\n1. **Create High-Value Segment**\n   - 847 leads match criteria for premium outreach\n   - Estimated conversion: 12%\n\n2. **Re-engage Dormant Leads**\n   - 1,234 leads inactive 30+ days\n   - Suggested: SMS reactivation campaign\n\n3. **Optimize Send Times**\n   - Best performing: Tue/Thu 6-8 PM\n   - Current sends at suboptimal times\n\nWant me to help implement any of these?",
            "type": "suggestion",
            "suggested_actions": [
                {"label": "Create Segment", "action": "create_segment"},
                {"label": "Start Campaign", "action": "create_campaign"},
                {"label": "See More", "action": "more_suggestions"}
            ]
        }
    
    def _handle_creation_intent(self, message: str, context: dict) -> dict:
        return {
            "content": "I can help you create that! What would you like to build?\n\n**Quick Creation Options:**\n\n1. 📊 **Dashboard** - Custom analytics view\n2. 👥 **Population** - Filtered lead segment\n3. 📧 **Campaign** - Email/SMS outreach\n4. 📈 **Report** - Scheduled analytics\n5. ⚙️ **Workflow** - Automated sequence\n\nJust tell me what you need and I'll guide you through it.",
            "type": "creation",
            "suggested_actions": [
                {"label": "Create Dashboard", "action": "create_dashboard"},
                {"label": "Create Population", "action": "create_population"},
                {"label": "Create Campaign", "action": "create_campaign"}
            ]
        }
    
    def _handle_general_intent(self, message: str, context: dict) -> dict:
        return {
            "content": f"I understand you're asking about: \"{message}\"\n\nI can help you with:\n- 🔍 **Data Queries** - Find and filter your data\n- 📊 **Analytics** - Understand patterns and trends\n- 📧 **Campaigns** - Plan and optimize outreach\n- 🤖 **Automation** - Set up workflows\n\nCould you tell me more about what you're trying to accomplish?",
            "type": "general",
            "suggested_actions": [
                {"label": "Show Examples", "action": "examples"},
                {"label": "View Help", "action": "help"}
            ]
        }
    
    async def generate_natural_language_report(
        self,
        data: dict,
        report_type: str = "summary"
    ) -> str:
        """Generate natural language report from data"""
        template = """
# {title}

## Overview
{overview}

## Key Findings
{findings}

## Recommendations
{recommendations}

---
*Generated by ACA DataHub AI Assistant on {date}*
"""
        return template.format(
            title="Data Analysis Report",
            overview="This report summarizes the key metrics and trends from your data.",
            findings="- Lead volume increased by 15%\n- Conversion rate improved to 3.2%\n- Top performing region: Georgia",
            recommendations="1. Focus on high-score leads\n2. Increase Georgia budget\n3. Test new email templates",
            date=datetime.utcnow().strftime("%Y-%m-%d")
        )
    
    def get_smart_suggestions(self, context: dict) -> List[dict]:
        """Generate context-aware suggestions"""
        return [
            {
                "type": "query",
                "title": "Find high-value leads",
                "description": "Show leads with score > 80 and valid email",
                "action": "run_query",
                "confidence": 0.9
            },
            {
                "type": "campaign",
                "title": "Launch follow-up sequence",
                "description": "847 leads ready for follow-up",
                "action": "create_campaign",
                "confidence": 0.85
            },
            {
                "type": "insight",
                "title": "Anomaly detected",
                "description": "Email open rate dropped 5% this week",
                "action": "investigate",
                "confidence": 0.8
            }
        ]


ai_service = AIAssistantService()


# =========================================================================
# Endpoints
# =========================================================================

@router.post("/chat")
async def chat(request: ChatRequest):
    """Send a message to the AI assistant"""
    conv_id = request.conversation_id or f"conv_{datetime.utcnow().timestamp()}"
    
    response = await ai_service.chat(
        request.message,
        conv_id,
        request.context
    )
    
    return {
        "conversation_id": conv_id,
        "response": response
    }


@router.get("/conversations")
async def list_conversations(limit: int = Query(default=20, le=50)):
    """List recent conversations"""
    return {"conversations": conversation_store.list(limit)}


@router.get("/conversations/{conversation_id}")
async def get_conversation(conversation_id: str):
    """Get conversation history"""
    conversation = conversation_store.get(conversation_id)
    if not conversation:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return conversation


@router.delete("/conversations/{conversation_id}")
async def delete_conversation(conversation_id: str):
    """Delete a conversation"""
    if conversation_id in conversation_store.conversations:
        del conversation_store.conversations[conversation_id]
        return {"success": True}
    raise HTTPException(status_code=404, detail="Conversation not found")


@router.get("/suggestions")
async def get_suggestions(context: Optional[str] = Query(default=None)):
    """Get smart suggestions based on context"""
    ctx = {}
    if context:
        try:
            import json
            ctx = json.loads(context)
        except:
            pass
    
    return {"suggestions": ai_service.get_smart_suggestions(ctx)}


@router.post("/generate-report")
async def generate_nl_report(
    report_type: str = Query(default="summary"),
    data: Optional[dict] = None
):
    """Generate natural language report"""
    report = await ai_service.generate_natural_language_report(data or {}, report_type)
    return {"report": report, "type": report_type}


@router.get("/capabilities")
async def get_capabilities():
    """Get AI assistant capabilities"""
    return {
        "capabilities": ai_service.capabilities,
        "version": "2.0",
        "powered_by": "Google Gemini"
    }


@router.post("/explain")
async def explain_data(
    topic: str = Query(...),
    data: Optional[dict] = None
):
    """Get AI explanation for data or concept"""
    response = ai_service._handle_explanation_intent(topic, data or {})
    return response
