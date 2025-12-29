"""
Conversational BI Routes for ACA DataHub
Natural language queries, voice-to-query, auto-visualization, and insight summarization
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum
import random
import re

router = APIRouter(prefix="/conversational-bi", tags=["Conversational BI"])


# =========================================================================
# Models
# =========================================================================

class VisualizationType(str, Enum):
    BAR_CHART = "bar_chart"
    LINE_CHART = "line_chart"
    PIE_CHART = "pie_chart"
    TABLE = "table"
    KPI_CARD = "kpi_card"
    SCATTER_PLOT = "scatter_plot"
    HEATMAP = "heatmap"


class QueryIntent(str, Enum):
    COUNT = "count"
    SUM = "sum"
    AVERAGE = "average"
    TREND = "trend"
    COMPARISON = "comparison"
    DISTRIBUTION = "distribution"
    TOP_N = "top_n"
    FILTER = "filter"


# =========================================================================
# Natural Language Query Engine
# =========================================================================

class NLQueryEngine:
    """Convert natural language to SQL/analytics queries"""
    
    def __init__(self):
        self.query_cache: Dict[str, dict] = {}
        self.conversation_context: Dict[str, List[dict]] = {}
        self._counter = 0
    
    def parse_query(self, natural_query: str) -> dict:
        """Parse natural language query into structured format"""
        query_lower = natural_query.lower()
        
        # Detect intent
        if any(word in query_lower for word in ["how many", "count", "number of"]):
            intent = QueryIntent.COUNT
        elif any(word in query_lower for word in ["total", "sum"]):
            intent = QueryIntent.SUM
        elif any(word in query_lower for word in ["average", "avg", "mean"]):
            intent = QueryIntent.AVERAGE
        elif any(word in query_lower for word in ["trend", "over time", "by month", "by week"]):
            intent = QueryIntent.TREND
        elif any(word in query_lower for word in ["compare", "vs", "versus"]):
            intent = QueryIntent.COMPARISON
        elif any(word in query_lower for word in ["distribution", "breakdown"]):
            intent = QueryIntent.DISTRIBUTION
        elif any(word in query_lower for word in ["top", "best", "highest", "lowest"]):
            intent = QueryIntent.TOP_N
        else:
            intent = QueryIntent.FILTER
        
        # Extract entities
        entities = self._extract_entities(natural_query)
        
        # Extract time range
        time_range = self._extract_time_range(natural_query)
        
        # Extract filters
        filters = self._extract_filters(natural_query)
        
        return {
            "original_query": natural_query,
            "intent": intent.value,
            "entities": entities,
            "time_range": time_range,
            "filters": filters,
            "confidence": round(random.uniform(0.75, 0.98), 3)
        }
    
    def _extract_entities(self, query: str) -> List[dict]:
        entities = []
        query_lower = query.lower()
        
        # Detect metric entities
        metrics = ["leads", "customers", "revenue", "campaigns", "populations", "users"]
        for metric in metrics:
            if metric in query_lower:
                entities.append({"type": "metric", "value": metric})
        
        # Detect dimension entities
        dimensions = ["state", "age", "income", "source", "status"]
        for dim in dimensions:
            if dim in query_lower:
                entities.append({"type": "dimension", "value": dim})
        
        return entities
    
    def _extract_time_range(self, query: str) -> Optional[dict]:
        query_lower = query.lower()
        
        if "today" in query_lower:
            return {"period": "today", "unit": "day"}
        elif "this week" in query_lower:
            return {"period": "this_week", "unit": "week"}
        elif "this month" in query_lower:
            return {"period": "this_month", "unit": "month"}
        elif "last month" in query_lower:
            return {"period": "last_month", "unit": "month"}
        elif "this year" in query_lower:
            return {"period": "this_year", "unit": "year"}
        elif "last 30 days" in query_lower:
            return {"period": "last_30_days", "unit": "day"}
        
        return None
    
    def _extract_filters(self, query: str) -> List[dict]:
        filters = []
        query_lower = query.lower()
        
        # State filter
        states = ["georgia", "florida", "texas", "california", "new york"]
        for state in states:
            if state in query_lower:
                filters.append({"field": "state", "operator": "eq", "value": state.title()})
        
        # Score filter
        if "high score" in query_lower or "score above" in query_lower:
            filters.append({"field": "score", "operator": "gte", "value": 70})
        
        return filters
    
    def generate_sql(self, parsed: dict) -> str:
        """Generate SQL from parsed query"""
        intent = parsed["intent"]
        entities = parsed.get("entities", [])
        filters = parsed.get("filters", [])
        
        # Get primary metric
        metric = next((e["value"] for e in entities if e["type"] == "metric"), "leads")
        dimension = next((e["value"] for e in entities if e["type"] == "dimension"), None)
        
        # Build SQL
        if intent == "count":
            sql = f"SELECT COUNT(*) as count FROM {metric}"
        elif intent == "sum":
            sql = f"SELECT SUM(value) as total FROM {metric}"
        elif intent == "average":
            sql = f"SELECT AVG(score) as average FROM {metric}"
        elif intent == "trend":
            sql = f"SELECT DATE(created_at) as date, COUNT(*) as count FROM {metric} GROUP BY DATE(created_at)"
        elif intent == "top_n":
            sql = f"SELECT * FROM {metric} ORDER BY score DESC LIMIT 10"
        else:
            sql = f"SELECT * FROM {metric}"
        
        # Add WHERE clause
        if filters:
            where_clauses = []
            for f in filters:
                where_clauses.append(f"{f['field']} {f['operator']} '{f['value']}'")
            sql += " WHERE " + " AND ".join(where_clauses)
        
        return sql
    
    def execute_query(self, natural_query: str, session_id: str = None) -> dict:
        """Full query execution pipeline"""
        self._counter += 1
        query_id = f"q_{self._counter}"
        
        # Parse
        parsed = self.parse_query(natural_query)
        
        # Generate SQL
        sql = self.generate_sql(parsed)
        
        # Simulate results
        intent = parsed["intent"]
        if intent == "count":
            result_data = {"count": random.randint(100, 10000)}
        elif intent == "sum":
            result_data = {"total": round(random.uniform(10000, 1000000), 2)}
        elif intent == "average":
            result_data = {"average": round(random.uniform(50, 90), 2)}
        elif intent == "trend":
            result_data = {
                "data": [
                    {"date": f"2024-12-{i:02d}", "count": random.randint(50, 200)}
                    for i in range(1, 28)
                ]
            }
        else:
            result_data = {
                "rows": random.randint(50, 500),
                "sample": [{"id": i, "name": f"Lead {i}", "score": random.randint(30, 100)} for i in range(5)]
            }
        
        # Store in context for follow-ups
        if session_id:
            if session_id not in self.conversation_context:
                self.conversation_context[session_id] = []
            self.conversation_context[session_id].append({
                "query": natural_query,
                "parsed": parsed,
                "result": result_data
            })
        
        # Cache
        self.query_cache[query_id] = {
            "query_id": query_id,
            "natural_query": natural_query,
            "parsed": parsed,
            "sql": sql,
            "result": result_data,
            "executed_at": datetime.utcnow().isoformat()
        }
        
        return self.query_cache[query_id]
    
    def get_suggestions(self, partial_query: str, session_id: str = None) -> List[dict]:
        """Get query suggestions based on partial input"""
        suggestions = []
        partial_lower = partial_query.lower()
        
        common_queries = [
            "How many leads do we have this month?",
            "Show me leads by state",
            "What is the average lead score?",
            "Top 10 leads by score",
            "Trend of new leads over the last 30 days",
            "Compare leads by source",
            "Distribution of leads by income bracket",
            "How many campaigns were sent this week?"
        ]
        
        for query in common_queries:
            if partial_lower in query.lower():
                suggestions.append({
                    "text": query,
                    "type": "common",
                    "confidence": 0.8
                })
        
        # Add context-based suggestions
        if session_id and session_id in self.conversation_context:
            context = self.conversation_context[session_id][-1] if self.conversation_context[session_id] else None
            if context:
                entities = context["parsed"].get("entities", [])
                for entity in entities:
                    if entity["type"] == "metric":
                        suggestions.append({
                            "text": f"Show me {entity['value']} breakdown by state",
                            "type": "contextual",
                            "confidence": 0.9
                        })
        
        return suggestions[:5]


nl_engine = NLQueryEngine()


# =========================================================================
# Auto-Visualization
# =========================================================================

class AutoVisualizer:
    """Automatically suggest and generate visualizations"""
    
    def recommend_visualization(self, query_result: dict) -> dict:
        """Recommend best visualization for query result"""
        intent = query_result.get("parsed", {}).get("intent", "")
        result = query_result.get("result", {})
        
        if intent == "count" or intent == "sum" or intent == "average":
            viz_type = VisualizationType.KPI_CARD
            config = {
                "title": "Result",
                "value": list(result.values())[0] if result else 0,
                "format": "number"
            }
        elif intent == "trend":
            viz_type = VisualizationType.LINE_CHART
            config = {
                "x_axis": "date",
                "y_axis": "count",
                "title": "Trend Over Time"
            }
        elif intent == "comparison":
            viz_type = VisualizationType.BAR_CHART
            config = {
                "x_axis": "category",
                "y_axis": "value",
                "title": "Comparison"
            }
        elif intent == "distribution":
            viz_type = VisualizationType.PIE_CHART
            config = {
                "label_field": "category",
                "value_field": "count",
                "title": "Distribution"
            }
        elif intent == "top_n":
            viz_type = VisualizationType.BAR_CHART
            config = {
                "x_axis": "name",
                "y_axis": "score",
                "title": "Top Performers",
                "horizontal": True
            }
        else:
            viz_type = VisualizationType.TABLE
            config = {"columns": ["id", "name", "value"]}
        
        return {
            "recommended": viz_type.value,
            "config": config,
            "alternatives": [
                VisualizationType.TABLE.value,
                VisualizationType.BAR_CHART.value
            ]
        }


auto_viz = AutoVisualizer()


# =========================================================================
# Insight Generator
# =========================================================================

class InsightGenerator:
    """Generate natural language insights from data"""
    
    def summarize_results(self, query_result: dict) -> dict:
        """Generate natural language summary of results"""
        intent = query_result.get("parsed", {}).get("intent", "")
        result = query_result.get("result", {})
        original_query = query_result.get("natural_query", "")
        
        insights = []
        
        if intent == "count":
            count = result.get("count", 0)
            insights.append(f"You have {count:,} records matching your query.")
            if count > 1000:
                insights.append("This is a substantial dataset. Consider filtering for more specific analysis.")
        
        elif intent == "average":
            avg = result.get("average", 0)
            insights.append(f"The average value is {avg:.1f}.")
            if avg > 80:
                insights.append("This is above the typical benchmark of 70.")
            elif avg < 50:
                insights.append("This is below average and may require attention.")
        
        elif intent == "trend":
            data = result.get("data", [])
            if len(data) > 1:
                first_val = data[0].get("count", 0)
                last_val = data[-1].get("count", 0)
                change = ((last_val - first_val) / first_val * 100) if first_val > 0 else 0
                
                if change > 10:
                    insights.append(f"There's an upward trend of {change:.1f}% over the period.")
                elif change < -10:
                    insights.append(f"There's a downward trend of {abs(change):.1f}% over the period.")
                else:
                    insights.append("The trend is relatively stable over the period.")
        
        # Add a recommendation
        recommendations = [
            "Consider segmenting by demographic for deeper insights.",
            "You might want to compare this with the previous period.",
            "Try filtering by high-value leads for more actionable data."
        ]
        
        return {
            "summary": " ".join(insights) if insights else "Query executed successfully.",
            "insights": insights,
            "recommendations": random.sample(recommendations, min(2, len(recommendations))),
            "follow_up_questions": self._generate_follow_ups(query_result)
        }
    
    def _generate_follow_ups(self, query_result: dict) -> List[str]:
        """Generate follow-up question suggestions"""
        intent = query_result.get("parsed", {}).get("intent", "")
        entities = query_result.get("parsed", {}).get("entities", [])
        
        follow_ups = []
        
        metric = next((e["value"] for e in entities if e["type"] == "metric"), "leads")
        
        if intent == "count":
            follow_ups.append(f"How has the number of {metric} changed over time?")
            follow_ups.append(f"What's the distribution of {metric} by state?")
        elif intent == "trend":
            follow_ups.append(f"What's driving the changes in {metric}?")
            follow_ups.append(f"Which segment shows the most growth?")
        else:
            follow_ups.append(f"Show me the top performing {metric}")
            follow_ups.append(f"Compare {metric} across different sources")
        
        return follow_ups[:3]


insight_gen = InsightGenerator()


# =========================================================================
# Endpoints
# =========================================================================

@router.post("/query")
async def execute_natural_query(
    query: str = Query(...),
    session_id: Optional[str] = Query(default=None)
):
    """Execute natural language query"""
    result = nl_engine.execute_query(query, session_id)
    
    # Add visualization recommendation
    result["visualization"] = auto_viz.recommend_visualization(result)
    
    # Add insights
    result["insights"] = insight_gen.summarize_results(result)
    
    return result


@router.get("/suggestions")
async def get_query_suggestions(
    partial: str = Query(default=""),
    session_id: Optional[str] = Query(default=None)
):
    """Get query suggestions"""
    return {"suggestions": nl_engine.get_suggestions(partial, session_id)}


@router.post("/parse")
async def parse_query(query: str = Query(...)):
    """Parse natural language query without executing"""
    return nl_engine.parse_query(query)


@router.get("/context/{session_id}")
async def get_conversation_context(session_id: str):
    """Get conversation context for session"""
    context = nl_engine.conversation_context.get(session_id, [])
    return {"session_id": session_id, "context": context[-10:]}  # Last 10 queries


@router.delete("/context/{session_id}")
async def clear_conversation_context(session_id: str):
    """Clear conversation context"""
    if session_id in nl_engine.conversation_context:
        del nl_engine.conversation_context[session_id]
    return {"success": True}


@router.post("/visualize")
async def recommend_visualization(query_result: dict):
    """Get visualization recommendation for query result"""
    return auto_viz.recommend_visualization(query_result)


@router.post("/summarize")
async def summarize_results(query_result: dict):
    """Generate natural language summary of results"""
    return insight_gen.summarize_results(query_result)


@router.get("/cache")
async def get_cached_queries(limit: int = Query(default=20)):
    """Get recently cached queries"""
    queries = list(nl_engine.query_cache.values())
    return {"queries": queries[-limit:]}
