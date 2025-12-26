"""
Messaging Service
Send messages to population members via email/SMS
"""

import json
import os
import re
import uuid
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum


class MessageChannel(str, Enum):
    EMAIL = "email"
    SMS = "sms"
    PREVIEW = "preview"


class SendStatus(str, Enum):
    PENDING = "pending"
    SENDING = "sending"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL = "partial"


@dataclass
class MessageTemplate:
    """Email/SMS template with variable placeholders"""
    id: str
    name: str
    subject: str  # For email
    body: str
    channel: MessageChannel
    variables: List[str] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class SendJob:
    """Record of a message send operation"""
    id: str
    template_id: str
    population_id: str
    channel: MessageChannel
    status: SendStatus
    total_recipients: int = 0
    sent_count: int = 0
    failed_count: int = 0
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    completed_at: Optional[str] = None
    errors: List[Dict] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        result = asdict(self)
        result['channel'] = self.channel.value
        result['status'] = self.status.value
        return result


class MessagingService:
    """Send messages to population members"""
    
    # Variable pattern: {{variable_name}}
    VARIABLE_PATTERN = re.compile(r'\{\{(\w+)\}\}')
    
    def __init__(self, job_id: str):
        self.job_id = job_id
        self.templates: Dict[str, MessageTemplate] = {}
        self.send_history: List[SendJob] = []
        self._storage_file = f"databases/{job_id}_messaging.json"
        self._load_data()
        
        # Email configuration (from environment)
        self.smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.smtp_user = os.getenv("SMTP_USER", "")
        self.smtp_pass = os.getenv("SMTP_PASS", "")
        self.from_email = os.getenv("FROM_EMAIL", "noreply@yxdbconverter.com")
    
    def _load_data(self):
        """Load saved templates and history"""
        if os.path.exists(self._storage_file):
            try:
                with open(self._storage_file, 'r') as f:
                    data = json.load(f)
                    for t in data.get('templates', []):
                        t['channel'] = MessageChannel(t['channel'])
                        template = MessageTemplate(**t)
                        self.templates[template.id] = template
                    for h in data.get('history', []):
                        h['channel'] = MessageChannel(h['channel'])
                        h['status'] = SendStatus(h['status'])
                        self.send_history.append(SendJob(**h))
            except Exception as e:
                print(f"Error loading messaging data: {e}")
    
    def _save_data(self):
        """Save templates and history"""
        os.makedirs("databases", exist_ok=True)
        with open(self._storage_file, 'w') as f:
            json.dump({
                'templates': [t.to_dict() for t in self.templates.values()],
                'history': [h.to_dict() for h in self.send_history]
            }, f, indent=2, default=str)
    
    def create_template(
        self,
        name: str,
        subject: str,
        body: str,
        channel: MessageChannel = MessageChannel.EMAIL
    ) -> MessageTemplate:
        """Create a new message template"""
        template_id = str(uuid.uuid4())[:12]
        
        # Extract variables from body and subject
        variables = list(set(
            self.VARIABLE_PATTERN.findall(subject) +
            self.VARIABLE_PATTERN.findall(body)
        ))
        
        template = MessageTemplate(
            id=template_id,
            name=name,
            subject=subject,
            body=body,
            channel=channel,
            variables=variables
        )
        
        self.templates[template_id] = template
        self._save_data()
        
        return template
    
    def get_template(self, template_id: str) -> Optional[MessageTemplate]:
        """Get a template by ID"""
        return self.templates.get(template_id)
    
    def list_templates(self) -> List[MessageTemplate]:
        """List all templates"""
        return list(self.templates.values())
    
    def delete_template(self, template_id: str) -> bool:
        """Delete a template"""
        if template_id in self.templates:
            del self.templates[template_id]
            self._save_data()
            return True
        return False
    
    def preview_message(
        self,
        template_id: str,
        sample_record: Dict
    ) -> Dict:
        """Preview a message with sample data"""
        template = self.templates.get(template_id)
        if not template:
            return {"error": "Template not found"}
        
        subject = self._substitute_variables(template.subject, sample_record)
        body = self._substitute_variables(template.body, sample_record)
        
        return {
            "template": template.to_dict(),
            "rendered": {
                "subject": subject,
                "body": body
            },
            "sample_data": sample_record,
            "missing_variables": [
                v for v in template.variables 
                if v not in sample_record
            ]
        }
    
    def _substitute_variables(self, text: str, data: Dict) -> str:
        """Replace {{variable}} with actual values"""
        def replace(match):
            var_name = match.group(1)
            value = data.get(var_name, f"[{var_name}]")
            return str(value) if value is not None else ""
        
        return self.VARIABLE_PATTERN.sub(replace, text)
    
    def send_to_population(
        self,
        population_manager,
        pop_id: str,
        template_id: str,
        email_column: str = "email",
        phone_column: str = "phone",
        dry_run: bool = False
    ) -> SendJob:
        """Send messages to all members of a population"""
        template = self.templates.get(template_id)
        if not template:
            raise ValueError("Template not found")
        
        population = population_manager.get_population(pop_id)
        if not population:
            raise ValueError("Population not found")
        
        # Get population data
        pop_data = population_manager.get_population_data(pop_id, limit=10000)
        records = pop_data.get("records", [])
        
        # Create send job
        send_job = SendJob(
            id=str(uuid.uuid4())[:12],
            template_id=template_id,
            population_id=pop_id,
            channel=template.channel,
            status=SendStatus.SENDING,
            total_recipients=len(records)
        )
        
        if dry_run:
            send_job.status = SendStatus.COMPLETED
            send_job.sent_count = len(records)
            send_job.completed_at = datetime.now().isoformat()
            self.send_history.append(send_job)
            self._save_data()
            return send_job
        
        # Send messages
        for record in records:
            try:
                if template.channel == MessageChannel.EMAIL:
                    email = record.get(email_column)
                    if email:
                        self._send_email(template, record, email)
                        send_job.sent_count += 1
                    else:
                        send_job.failed_count += 1
                        send_job.errors.append({
                            "record_id": record.get("id", "unknown"),
                            "error": "No email address"
                        })
                
                elif template.channel == MessageChannel.SMS:
                    phone = record.get(phone_column)
                    if phone:
                        self._send_sms(template, record, phone)
                        send_job.sent_count += 1
                    else:
                        send_job.failed_count += 1
                        send_job.errors.append({
                            "record_id": record.get("id", "unknown"),
                            "error": "No phone number"
                        })
                        
            except Exception as e:
                send_job.failed_count += 1
                send_job.errors.append({
                    "record_id": record.get("id", "unknown"),
                    "error": str(e)
                })
        
        # Update status
        if send_job.failed_count == 0:
            send_job.status = SendStatus.COMPLETED
        elif send_job.sent_count == 0:
            send_job.status = SendStatus.FAILED
        else:
            send_job.status = SendStatus.PARTIAL
        
        send_job.completed_at = datetime.now().isoformat()
        self.send_history.append(send_job)
        self._save_data()
        
        return send_job
    
    def _send_email(self, template: MessageTemplate, record: Dict, to_email: str):
        """Send an email using SMTP"""
        if not self.smtp_user or not self.smtp_pass:
            # Log to file instead of actually sending
            self._log_message("email", to_email, template, record)
            return
        
        subject = self._substitute_variables(template.subject, record)
        body = self._substitute_variables(template.body, record)
        
        msg = MIMEMultipart()
        msg['From'] = self.from_email
        msg['To'] = to_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        
        with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
            server.starttls()
            server.login(self.smtp_user, self.smtp_pass)
            server.send_message(msg)
    
    def _send_sms(self, template: MessageTemplate, record: Dict, phone: str):
        """Send SMS (placeholder - would integrate with Twilio etc)"""
        # Log to file instead of actually sending
        self._log_message("sms", phone, template, record)
    
    def _log_message(self, channel: str, recipient: str, template: MessageTemplate, record: Dict):
        """Log message to file for testing"""
        os.makedirs("messages", exist_ok=True)
        log_file = f"messages/{self.job_id}_messages.log"
        
        subject = self._substitute_variables(template.subject, record)
        body = self._substitute_variables(template.body, record)
        
        with open(log_file, 'a') as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"Channel: {channel}\n")
            f.write(f"To: {recipient}\n")
            f.write(f"Time: {datetime.now().isoformat()}\n")
            f.write(f"Subject: {subject}\n")
            f.write(f"Body:\n{body}\n")
    
    def get_send_history(self, pop_id: str = None) -> List[SendJob]:
        """Get send history, optionally filtered by population"""
        if pop_id:
            return [h for h in self.send_history if h.population_id == pop_id]
        return self.send_history


# Default templates
DEFAULT_TEMPLATES = [
    {
        "name": "Welcome Email",
        "subject": "Welcome, {{name}}!",
        "body": """Hello {{name}},

Welcome to our platform! We're excited to have you on board.

Your account details:
- Email: {{email}}
- Region: {{region}}

If you have any questions, please don't hesitate to reach out.

Best regards,
The Team""",
        "channel": MessageChannel.EMAIL
    },
    {
        "name": "Sales Follow-up",
        "subject": "Following up on your recent activity",
        "body": """Hi {{name}},

We noticed your recent activity and wanted to reach out.

Your current sales: ${{sales}}
Region: {{region}}

Would you like to schedule a call to discuss how we can help you grow?

Best,
Sales Team""",
        "channel": MessageChannel.EMAIL
    },
    {
        "name": "Alert Notification",
        "subject": "Important: Action Required",
        "body": """Dear {{name}},

This is an automated alert regarding your account.

Please review and take action if necessary.

Thank you.""",
        "channel": MessageChannel.EMAIL
    }
]


# Global storage for messaging services per job
_messaging_services: Dict[str, MessagingService] = {}


def get_messaging_service(job_id: str) -> MessagingService:
    """Get or create a messaging service for a job"""
    if job_id not in _messaging_services:
        service = MessagingService(job_id)
        # Add default templates if none exist
        if not service.templates:
            for t in DEFAULT_TEMPLATES:
                service.create_template(**t)
        _messaging_services[job_id] = service
    return _messaging_services[job_id]
