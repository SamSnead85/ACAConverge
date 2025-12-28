"""
Workflow Automation Routes for ACA DataHub
Visual workflow builder, triggers, conditions, and execution
"""

from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from enum import Enum
import json

router = APIRouter(prefix="/workflows", tags=["Workflows"])


# =========================================================================
# Models
# =========================================================================

class TriggerType(str, Enum):
    MANUAL = "manual"
    SCHEDULE = "schedule"
    WEBHOOK = "webhook"
    DATA_CHANGE = "data_change"
    THRESHOLD = "threshold"
    EVENT = "event"


class ActionType(str, Enum):
    SEND_EMAIL = "send_email"
    SEND_SMS = "send_sms"
    CREATE_POPULATION = "create_population"
    RUN_QUERY = "run_query"
    WEBHOOK = "webhook"
    DELAY = "delay"
    CONDITION = "condition"
    UPDATE_RECORD = "update_record"
    NOTIFY = "notify"
    ASSIGN_SCORE = "assign_score"


class ExecutionStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"
    CANCELLED = "cancelled"


class WorkflowNode(BaseModel):
    id: str
    type: ActionType
    config: Dict[str, Any] = {}
    next_nodes: List[str] = []
    position: Dict[str, int] = {"x": 0, "y": 0}


class WorkflowCreate(BaseModel):
    name: str
    description: Optional[str] = None
    trigger: Dict[str, Any]
    nodes: List[WorkflowNode]
    is_active: bool = True


# =========================================================================
# Workflow Store
# =========================================================================

class WorkflowStore:
    """Manages workflow definitions and executions"""
    
    def __init__(self):
        self.workflows: Dict[str, dict] = {}
        self.executions: Dict[str, dict] = {}
        self._wf_counter = 0
        self._exec_counter = 0
        self._init_templates()
    
    def _init_templates(self):
        """Initialize workflow templates"""
        self.templates = [
            {
                "id": "tpl_lead_nurture",
                "name": "Lead Nurture Sequence",
                "description": "Automated follow-up sequence for new leads",
                "category": "Marketing",
                "nodes": [
                    {"type": "send_email", "label": "Welcome Email"},
                    {"type": "delay", "label": "Wait 3 days"},
                    {"type": "condition", "label": "Opened Email?"},
                    {"type": "send_email", "label": "Follow-up Email"}
                ]
            },
            {
                "id": "tpl_score_update",
                "name": "Score-Based Assignment",
                "description": "Assign leads to sales reps based on score",
                "category": "Sales",
                "nodes": [
                    {"type": "threshold", "label": "Score > 80?"},
                    {"type": "assign", "label": "Assign to Senior Rep"},
                    {"type": "notify", "label": "Send Notification"}
                ]
            },
            {
                "id": "tpl_data_quality",
                "name": "Data Quality Alert",
                "description": "Alert when data quality drops",
                "category": "Operations",
                "nodes": [
                    {"type": "threshold", "label": "Quality < 70%"},
                    {"type": "notify", "label": "Alert Admin"},
                    {"type": "webhook", "label": "Log to Slack"}
                ]
            }
        ]
    
    def create(self, data: dict) -> dict:
        self._wf_counter += 1
        workflow_id = f"wf_{self._wf_counter}"
        
        workflow = {
            "id": workflow_id,
            "status": "active" if data.get("is_active", True) else "paused",
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "run_count": 0,
            "last_run": None,
            **data
        }
        
        self.workflows[workflow_id] = workflow
        return workflow
    
    def get(self, workflow_id: str) -> Optional[dict]:
        return self.workflows.get(workflow_id)
    
    def list(self, status: str = None) -> List[dict]:
        workflows = list(self.workflows.values())
        if status:
            workflows = [w for w in workflows if w.get("status") == status]
        return sorted(workflows, key=lambda x: x.get("created_at", ""), reverse=True)
    
    def update(self, workflow_id: str, updates: dict) -> Optional[dict]:
        if workflow_id not in self.workflows:
            return None
        updates["updated_at"] = datetime.utcnow().isoformat()
        self.workflows[workflow_id].update(updates)
        return self.workflows[workflow_id]
    
    def delete(self, workflow_id: str) -> bool:
        if workflow_id in self.workflows:
            del self.workflows[workflow_id]
            return True
        return False
    
    def execute(self, workflow_id: str, input_data: dict = None) -> dict:
        """Start workflow execution"""
        workflow = self.get(workflow_id)
        if not workflow:
            raise ValueError("Workflow not found")
        
        self._exec_counter += 1
        execution_id = f"exec_{self._exec_counter}"
        
        execution = {
            "id": execution_id,
            "workflow_id": workflow_id,
            "status": ExecutionStatus.RUNNING.value,
            "started_at": datetime.utcnow().isoformat(),
            "completed_at": None,
            "input": input_data or {},
            "current_node": workflow.get("nodes", [{}])[0].get("id") if workflow.get("nodes") else None,
            "node_results": [],
            "error": None
        }
        
        self.executions[execution_id] = execution
        
        # Update workflow stats
        self.workflows[workflow_id]["run_count"] += 1
        self.workflows[workflow_id]["last_run"] = execution["started_at"]
        
        return execution
    
    def complete_execution(self, execution_id: str, status: str, error: str = None):
        if execution_id in self.executions:
            self.executions[execution_id]["status"] = status
            self.executions[execution_id]["completed_at"] = datetime.utcnow().isoformat()
            if error:
                self.executions[execution_id]["error"] = error
    
    def get_execution(self, execution_id: str) -> Optional[dict]:
        return self.executions.get(execution_id)
    
    def list_executions(self, workflow_id: str = None, limit: int = 50) -> List[dict]:
        executions = list(self.executions.values())
        if workflow_id:
            executions = [e for e in executions if e.get("workflow_id") == workflow_id]
        return sorted(executions, key=lambda x: x.get("started_at", ""), reverse=True)[:limit]


workflow_store = WorkflowStore()


# =========================================================================
# Workflow Engine
# =========================================================================

class WorkflowEngine:
    """Executes workflow nodes"""
    
    async def execute_node(self, node: dict, context: dict) -> dict:
        """Execute a single workflow node"""
        node_type = node.get("type")
        config = node.get("config", {})
        
        result = {
            "node_id": node.get("id"),
            "type": node_type,
            "started_at": datetime.utcnow().isoformat(),
            "status": "completed"
        }
        
        try:
            if node_type == ActionType.DELAY.value:
                # Simulate delay
                result["output"] = {"delayed_seconds": config.get("seconds", 0)}
            
            elif node_type == ActionType.SEND_EMAIL.value:
                result["output"] = {
                    "sent_to": config.get("to", context.get("email")),
                    "template": config.get("template"),
                    "status": "sent"
                }
            
            elif node_type == ActionType.CONDITION.value:
                # Evaluate condition
                field = config.get("field")
                operator = config.get("operator", "eq")
                value = config.get("value")
                actual = context.get(field)
                
                if operator == "eq":
                    passed = actual == value
                elif operator == "gt":
                    passed = float(actual) > float(value)
                elif operator == "lt":
                    passed = float(actual) < float(value)
                elif operator == "contains":
                    passed = value in str(actual)
                else:
                    passed = False
                
                result["output"] = {"passed": passed, "next_branch": "true" if passed else "false"}
            
            elif node_type == ActionType.WEBHOOK.value:
                result["output"] = {
                    "url": config.get("url"),
                    "status_code": 200,
                    "response": {"success": True}
                }
            
            elif node_type == ActionType.NOTIFY.value:
                result["output"] = {
                    "channel": config.get("channel", "in_app"),
                    "message": config.get("message"),
                    "recipients": config.get("recipients", [])
                }
            
            else:
                result["output"] = {"message": f"Executed {node_type}"}
        
        except Exception as e:
            result["status"] = "failed"
            result["error"] = str(e)
        
        result["completed_at"] = datetime.utcnow().isoformat()
        return result


workflow_engine = WorkflowEngine()


# =========================================================================
# Endpoints
# =========================================================================

@router.post("")
async def create_workflow(workflow: WorkflowCreate):
    """Create a new workflow"""
    result = workflow_store.create(workflow.dict())
    return {"success": True, "workflow": result}


@router.get("")
async def list_workflows(status: Optional[str] = Query(default=None)):
    """List all workflows"""
    return {"workflows": workflow_store.list(status)}


@router.get("/templates")
async def get_workflow_templates():
    """Get workflow templates"""
    return {"templates": workflow_store.templates}


@router.get("/{workflow_id}")
async def get_workflow(workflow_id: str):
    """Get a specific workflow"""
    workflow = workflow_store.get(workflow_id)
    if not workflow:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return workflow


@router.put("/{workflow_id}")
async def update_workflow(workflow_id: str, updates: dict):
    """Update a workflow"""
    result = workflow_store.update(workflow_id, updates)
    if not result:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return {"success": True, "workflow": result}


@router.delete("/{workflow_id}")
async def delete_workflow(workflow_id: str):
    """Delete a workflow"""
    if not workflow_store.delete(workflow_id):
        raise HTTPException(status_code=404, detail="Workflow not found")
    return {"success": True}


@router.post("/{workflow_id}/activate")
async def activate_workflow(workflow_id: str):
    """Activate a workflow"""
    result = workflow_store.update(workflow_id, {"status": "active"})
    if not result:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return {"success": True}


@router.post("/{workflow_id}/deactivate")
async def deactivate_workflow(workflow_id: str):
    """Deactivate a workflow"""
    result = workflow_store.update(workflow_id, {"status": "paused"})
    if not result:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return {"success": True}


@router.post("/{workflow_id}/execute")
async def execute_workflow(
    workflow_id: str,
    background_tasks: BackgroundTasks,
    input_data: Optional[dict] = None
):
    """Execute a workflow"""
    try:
        execution = workflow_store.execute(workflow_id, input_data)
        
        # Simulate completion after short delay
        workflow_store.complete_execution(execution["id"], ExecutionStatus.COMPLETED.value)
        
        return {"success": True, "execution": execution}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/{workflow_id}/executions")
async def list_workflow_executions(
    workflow_id: str,
    limit: int = Query(default=50, le=100)
):
    """List executions for a workflow"""
    return {"executions": workflow_store.list_executions(workflow_id, limit)}


@router.get("/executions/{execution_id}")
async def get_execution(execution_id: str):
    """Get execution details"""
    execution = workflow_store.get_execution(execution_id)
    if not execution:
        raise HTTPException(status_code=404, detail="Execution not found")
    return execution


@router.post("/executions/{execution_id}/cancel")
async def cancel_execution(execution_id: str):
    """Cancel a running execution"""
    execution = workflow_store.get_execution(execution_id)
    if not execution:
        raise HTTPException(status_code=404, detail="Execution not found")
    
    if execution["status"] != ExecutionStatus.RUNNING.value:
        raise HTTPException(status_code=400, detail="Execution is not running")
    
    workflow_store.complete_execution(execution_id, ExecutionStatus.CANCELLED.value)
    return {"success": True}
