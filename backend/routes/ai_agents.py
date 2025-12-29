"""
Advanced AI Agents Routes for ACA DataHub
Multi-agent orchestration, tool-calling, memory management, and safety guardrails
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from enum import Enum
import random
import asyncio

router = APIRouter(prefix="/agents", tags=["AI Agents"])


# =========================================================================
# Models
# =========================================================================

class AgentState(str, Enum):
    IDLE = "idle"
    THINKING = "thinking"
    EXECUTING = "executing"
    WAITING = "waiting"
    ERROR = "error"


class ToolType(str, Enum):
    QUERY = "query"
    ACTION = "action"
    RETRIEVAL = "retrieval"
    COMPUTATION = "computation"


# =========================================================================
# Agent Memory
# =========================================================================

class AgentMemory:
    """Manage agent context and memory"""
    
    def __init__(self):
        self.conversations: Dict[str, List[dict]] = {}
        self.working_memory: Dict[str, dict] = {}
        self.long_term: Dict[str, List[dict]] = {}
    
    def add_message(self, session_id: str, role: str, content: str, metadata: dict = None):
        if session_id not in self.conversations:
            self.conversations[session_id] = []
        
        message = {
            "role": role,
            "content": content,
            "metadata": metadata or {},
            "timestamp": datetime.utcnow().isoformat()
        }
        self.conversations[session_id].append(message)
        
        # Trim old messages
        if len(self.conversations[session_id]) > 100:
            self.conversations[session_id] = self.conversations[session_id][-100:]
        
        return message
    
    def get_context(self, session_id: str, limit: int = 20) -> List[dict]:
        return self.conversations.get(session_id, [])[-limit:]
    
    def store_fact(self, session_id: str, key: str, value: Any):
        if session_id not in self.working_memory:
            self.working_memory[session_id] = {}
        self.working_memory[session_id][key] = value
    
    def get_fact(self, session_id: str, key: str) -> Any:
        return self.working_memory.get(session_id, {}).get(key)
    
    def summarize_for_storage(self, session_id: str) -> dict:
        """Summarize conversation for long-term storage"""
        messages = self.conversations.get(session_id, [])
        
        summary = {
            "session_id": session_id,
            "message_count": len(messages),
            "topics": self._extract_topics(messages),
            "key_facts": self.working_memory.get(session_id, {}),
            "summarized_at": datetime.utcnow().isoformat()
        }
        
        if session_id not in self.long_term:
            self.long_term[session_id] = []
        self.long_term[session_id].append(summary)
        
        return summary
    
    def _extract_topics(self, messages: List[dict]) -> List[str]:
        topics = set()
        keywords = ["leads", "campaigns", "reports", "analytics", "data", "query", "export"]
        
        for msg in messages:
            content = msg.get("content", "").lower()
            for kw in keywords:
                if kw in content:
                    topics.add(kw)
        
        return list(topics)


memory = AgentMemory()


# =========================================================================
# Tool Registry
# =========================================================================

class ToolRegistry:
    """Registry of tools available to agents"""
    
    def __init__(self):
        self.tools: Dict[str, dict] = {}
        self._init_tools()
    
    def _init_tools(self):
        self.tools = {
            "query_database": {
                "name": "query_database",
                "description": "Execute SQL query against the database",
                "type": ToolType.QUERY.value,
                "parameters": {"query": "string"},
                "required_permissions": ["data:read"]
            },
            "create_report": {
                "name": "create_report",
                "description": "Generate a report based on data",
                "type": ToolType.ACTION.value,
                "parameters": {"title": "string", "data": "object"},
                "required_permissions": ["reports:write"]
            },
            "search_knowledge": {
                "name": "search_knowledge",
                "description": "Search the knowledge base for relevant information",
                "type": ToolType.RETRIEVAL.value,
                "parameters": {"query": "string"},
                "required_permissions": ["knowledge:read"]
            },
            "calculate_metrics": {
                "name": "calculate_metrics",
                "description": "Calculate statistical metrics on data",
                "type": ToolType.COMPUTATION.value,
                "parameters": {"data": "array", "metrics": "array"},
                "required_permissions": ["compute"]
            },
            "send_notification": {
                "name": "send_notification",
                "description": "Send notification to users",
                "type": ToolType.ACTION.value,
                "parameters": {"recipients": "array", "message": "string"},
                "required_permissions": ["notifications:write"]
            }
        }
    
    def get_tool(self, name: str) -> Optional[dict]:
        return self.tools.get(name)
    
    def list_tools(self, tool_type: str = None) -> List[dict]:
        tools = list(self.tools.values())
        if tool_type:
            tools = [t for t in tools if t["type"] == tool_type]
        return tools
    
    def execute_tool(self, name: str, parameters: dict) -> dict:
        """Execute a tool with given parameters"""
        tool = self.tools.get(name)
        if not tool:
            return {"error": "Tool not found"}
        
        # Simulate tool execution
        result = {
            "tool": name,
            "parameters": parameters,
            "success": True,
            "result": self._simulate_result(name, parameters),
            "executed_at": datetime.utcnow().isoformat(),
            "duration_ms": random.randint(50, 500)
        }
        
        return result
    
    def _simulate_result(self, name: str, params: dict) -> Any:
        if name == "query_database":
            return {"rows": random.randint(10, 100), "columns": 5}
        elif name == "search_knowledge":
            return {"documents": random.randint(1, 10), "relevance": 0.85}
        elif name == "calculate_metrics":
            return {"mean": 42.5, "median": 40, "std": 12.3}
        else:
            return {"status": "completed"}


tools = ToolRegistry()


# =========================================================================
# Agent Orchestrator
# =========================================================================

class AgentOrchestrator:
    """Orchestrate multiple AI agents"""
    
    def __init__(self):
        self.agents: Dict[str, dict] = {}
        self.executions: List[dict] = []
        self._counter = 0
        self._init_agents()
    
    def _init_agents(self):
        self.agents = {
            "analyst": {
                "id": "analyst",
                "name": "Data Analyst Agent",
                "description": "Analyzes data and generates insights",
                "capabilities": ["query_database", "calculate_metrics"],
                "state": AgentState.IDLE.value
            },
            "reporter": {
                "id": "reporter",
                "name": "Report Generator Agent",
                "description": "Creates reports and visualizations",
                "capabilities": ["create_report", "query_database"],
                "state": AgentState.IDLE.value
            },
            "researcher": {
                "id": "researcher",
                "name": "Research Agent",
                "description": "Searches knowledge base and gathers information",
                "capabilities": ["search_knowledge"],
                "state": AgentState.IDLE.value
            },
            "coordinator": {
                "id": "coordinator",
                "name": "Coordinator Agent",
                "description": "Orchestrates other agents and manages workflow",
                "capabilities": ["send_notification"],
                "state": AgentState.IDLE.value
            }
        }
    
    def get_agent(self, agent_id: str) -> Optional[dict]:
        return self.agents.get(agent_id)
    
    async def execute_task(
        self,
        task: str,
        session_id: str,
        agent_id: str = None
    ) -> dict:
        """Execute a task using agents"""
        self._counter += 1
        execution_id = f"exec_{self._counter}"
        
        # Select appropriate agent
        selected_agent = agent_id or self._select_agent(task)
        agent = self.agents.get(selected_agent)
        
        if not agent:
            return {"error": "No suitable agent found"}
        
        # Update agent state
        agent["state"] = AgentState.THINKING.value
        
        # Store user message
        memory.add_message(session_id, "user", task)
        
        # Plan execution
        plan = self._create_plan(task, agent)
        
        # Execute plan
        agent["state"] = AgentState.EXECUTING.value
        results = []
        
        for step in plan["steps"]:
            tool_result = tools.execute_tool(step["tool"], step["parameters"])
            results.append(tool_result)
        
        # Generate response
        response = self._generate_response(task, results)
        memory.add_message(session_id, "assistant", response)
        
        # Reset agent state
        agent["state"] = AgentState.IDLE.value
        
        execution = {
            "id": execution_id,
            "task": task,
            "session_id": session_id,
            "agent": selected_agent,
            "plan": plan,
            "results": results,
            "response": response,
            "executed_at": datetime.utcnow().isoformat()
        }
        
        self.executions.append(execution)
        return execution
    
    def _select_agent(self, task: str) -> str:
        task_lower = task.lower()
        
        if any(w in task_lower for w in ["analyze", "metrics", "statistics"]):
            return "analyst"
        elif any(w in task_lower for w in ["report", "chart", "visualization"]):
            return "reporter"
        elif any(w in task_lower for w in ["find", "search", "look up"]):
            return "researcher"
        else:
            return "coordinator"
    
    def _create_plan(self, task: str, agent: dict) -> dict:
        """Create execution plan for task"""
        capabilities = agent.get("capabilities", [])
        
        steps = []
        for cap in capabilities[:2]:  # Use first 2 capabilities
            steps.append({
                "tool": cap,
                "parameters": {"query": task},
                "order": len(steps) + 1
            })
        
        return {
            "agent": agent["id"],
            "task": task,
            "steps": steps,
            "created_at": datetime.utcnow().isoformat()
        }
    
    def _generate_response(self, task: str, results: List[dict]) -> str:
        if not results:
            return "I was unable to complete the task."
        
        success_count = sum(1 for r in results if r.get("success"))
        
        if success_count == len(results):
            return f"I've completed your request. Executed {len(results)} operations successfully."
        else:
            return f"Completed {success_count} of {len(results)} operations."


orchestrator = AgentOrchestrator()


# =========================================================================
# Safety Guardrails
# =========================================================================

class SafetyGuardrails:
    """Implement safety checks for AI agents"""
    
    def __init__(self):
        self.blocked_patterns: List[str] = [
            "delete all",
            "drop table",
            "truncate",
            "rm -rf",
            "shutdown"
        ]
        self.rate_limits: Dict[str, dict] = {}
        self.violations: List[dict] = []
    
    def check_input(self, input_text: str, user_id: str) -> dict:
        """Check input for safety violations"""
        violations = []
        
        # Check for blocked patterns
        input_lower = input_text.lower()
        for pattern in self.blocked_patterns:
            if pattern in input_lower:
                violations.append({
                    "type": "blocked_pattern",
                    "pattern": pattern,
                    "severity": "high"
                })
        
        # Check rate limits
        if user_id in self.rate_limits:
            limit_info = self.rate_limits[user_id]
            if limit_info.get("count", 0) > 100:
                violations.append({
                    "type": "rate_limit",
                    "severity": "medium"
                })
        
        # Update rate limit counter
        if user_id not in self.rate_limits:
            self.rate_limits[user_id] = {"count": 0, "window_start": datetime.utcnow().isoformat()}
        self.rate_limits[user_id]["count"] = self.rate_limits[user_id].get("count", 0) + 1
        
        is_safe = len(violations) == 0
        
        if not is_safe:
            self.violations.append({
                "user_id": user_id,
                "input": input_text[:100],
                "violations": violations,
                "timestamp": datetime.utcnow().isoformat()
            })
        
        return {
            "safe": is_safe,
            "violations": violations,
            "checked_at": datetime.utcnow().isoformat()
        }
    
    def check_output(self, output_text: str) -> dict:
        """Check agent output for sensitive content"""
        sensitive_patterns = ["password", "api_key", "secret", "ssn", "credit_card"]
        found = []
        
        output_lower = output_text.lower()
        for pattern in sensitive_patterns:
            if pattern in output_lower:
                found.append(pattern)
        
        return {
            "safe": len(found) == 0,
            "sensitive_content_detected": found,
            "action": "redact" if found else "allow"
        }


guardrails = SafetyGuardrails()


# =========================================================================
# Endpoints
# =========================================================================

@router.get("/")
async def list_agents():
    """List available agents"""
    return {"agents": list(orchestrator.agents.values())}


@router.get("/{agent_id}")
async def get_agent(agent_id: str):
    """Get agent details"""
    agent = orchestrator.get_agent(agent_id)
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
    return agent


@router.post("/execute")
async def execute_task(
    task: str = Query(...),
    session_id: str = Query(...),
    agent_id: Optional[str] = Query(default=None)
):
    """Execute task with AI agent"""
    # Safety check
    safety = guardrails.check_input(task, session_id)
    if not safety["safe"]:
        raise HTTPException(status_code=400, detail=f"Safety violation: {safety['violations']}")
    
    result = await orchestrator.execute_task(task, session_id, agent_id)
    return result


@router.get("/memory/{session_id}")
async def get_conversation(session_id: str, limit: int = Query(default=20)):
    """Get conversation history"""
    return {"messages": memory.get_context(session_id, limit)}


@router.post("/memory/{session_id}/summarize")
async def summarize_conversation(session_id: str):
    """Summarize conversation for long-term storage"""
    return memory.summarize_for_storage(session_id)


@router.delete("/memory/{session_id}")
async def clear_conversation(session_id: str):
    """Clear conversation history"""
    if session_id in memory.conversations:
        del memory.conversations[session_id]
    return {"success": True}


@router.get("/tools")
async def list_tools(tool_type: Optional[str] = Query(default=None)):
    """List available tools"""
    return {"tools": tools.list_tools(tool_type)}


@router.post("/tools/{tool_name}/execute")
async def execute_tool(tool_name: str, parameters: dict = None):
    """Execute a specific tool"""
    result = tools.execute_tool(tool_name, parameters or {})
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.get("/executions")
async def list_executions(limit: int = Query(default=20)):
    """List recent executions"""
    return {"executions": orchestrator.executions[-limit:]}


@router.post("/safety/check")
async def check_safety(
    input_text: str = Query(...),
    user_id: str = Query(default="anonymous")
):
    """Check input for safety violations"""
    return guardrails.check_input(input_text, user_id)


@router.get("/safety/violations")
async def get_violations(limit: int = Query(default=50)):
    """Get safety violation history"""
    return {"violations": guardrails.violations[-limit:]}
