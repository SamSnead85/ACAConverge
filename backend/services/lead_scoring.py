"""
Lead Scoring Service
AI-powered lead analysis and scoring for ACA enrollment sales
"""

import os
import json
import sqlite3
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from datetime import datetime


@dataclass
class LeadScore:
    """Lead scoring result"""
    record_id: int
    score: float  # 0-100
    tier: str  # hot, warm, cold
    reasons: List[str]
    email: Optional[str] = None
    phone: Optional[str] = None
    name: Optional[str] = None
    recommended_action: str = ""


@dataclass
class LeadInsight:
    """AI-generated insight about leads"""
    insight_type: str  # opportunity, risk, recommendation
    title: str
    description: str
    affected_count: int
    action_query: str  # Natural language query to explore this insight


class LeadScoringService:
    """
    AI-powered lead scoring and analysis for ACA enrollment
    Uses Gemini for intelligent lead identification
    """

    def __init__(self, db_path: str, table_name: str = "converted_data"):
        self.db_path = db_path
        self.table_name = table_name
        self.schema = []
        self._init_gemini()
        self._load_schema()

    def _init_gemini(self):
        """Initialize Gemini API"""
        self.gemini_model = None
        api_key = os.getenv("GEMINI_API_KEY")
        
        if api_key:
            try:
                import google.generativeai as genai
                genai.configure(api_key=api_key)
                self.gemini_model = genai.GenerativeModel('gemini-2.0-flash')
            except Exception as e:
                print(f"Warning: Could not initialize Gemini: {e}")

    def _load_schema(self):
        """Load database schema"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(f'PRAGMA table_info("{self.table_name}")')
            self.schema = [
                {"name": row[1], "type": row[2]}
                for row in cursor.fetchall()
            ]
            conn.close()
        except Exception as e:
            print(f"Error loading schema: {e}")

    def _identify_key_columns(self) -> Dict[str, str]:
        """Use AI to identify important columns for lead scoring"""
        columns = [col['name'] for col in self.schema]
        
        # Heuristic detection
        key_cols = {
            'email': None,
            'phone': None,
            'name': None,
            'income': None,
            'age': None,
            'state': None,
            'insurance_status': None,
            'enrollment_date': None
        }
        
        for col in columns:
            col_lower = col.lower()
            if 'email' in col_lower:
                key_cols['email'] = col
            elif 'phone' in col_lower or 'mobile' in col_lower:
                key_cols['phone'] = col
            elif 'name' in col_lower and 'first' in col_lower:
                key_cols['name'] = col
            elif key_cols['name'] is None and 'name' in col_lower:
                key_cols['name'] = col
            elif 'income' in col_lower or 'salary' in col_lower:
                key_cols['income'] = col
            elif 'age' in col_lower or 'birth' in col_lower or 'dob' in col_lower:
                key_cols['age'] = col
            elif 'state' in col_lower and 'insurance' not in col_lower:
                key_cols['state'] = col
            elif 'insurance' in col_lower or 'coverage' in col_lower or 'enrolled' in col_lower:
                key_cols['insurance_status'] = col
        
        return {k: v for k, v in key_cols.items() if v}

    def get_best_leads(
        self, 
        limit: int = 100, 
        criteria: str = "ACA enrollment"
    ) -> Dict[str, Any]:
        """
        Use AI to identify the best leads for a given criteria
        
        Args:
            limit: Number of leads to return
            criteria: What type of leads to find (e.g., "ACA enrollment")
        
        Returns:
            Dict with leads, insights, and recommended query
        """
        key_cols = self._identify_key_columns()
        columns = [col['name'] for col in self.schema]
        
        # Build AI prompt
        if self.gemini_model:
            prompt = f"""You are an expert at identifying sales leads for {criteria}.

Given this database schema:
Columns: {', '.join(columns)}

Key identified columns:
{json.dumps(key_cols, indent=2)}

Generate a SQL query to find the top {limit} best leads for {criteria}.

Consider:
1. People who likely qualify for ACA (income-based if available)
2. People without current insurance coverage (if that data exists)
3. People with valid contact information (email or phone)
4. Prioritize by lead quality indicators

Return ONLY a valid SQLite SELECT query. Include a LIMIT {limit} clause.
The table name is "{self.table_name}".
"""
            try:
                response = self.gemini_model.generate_content(prompt)
                sql = response.text.strip()
                
                # Clean SQL
                sql = sql.replace('```sql', '').replace('```', '').strip()
                if not sql.upper().startswith('SELECT'):
                    sql = f'SELECT * FROM "{self.table_name}" LIMIT {limit}'
                
            except Exception as e:
                # Fallback query
                sql = self._build_fallback_query(key_cols, limit)
        else:
            sql = self._build_fallback_query(key_cols, limit)
        
        # Execute query
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            conn.close()
            
            leads = [dict(row) for row in rows]
            
            # Generate insights about the leads
            insights = self._generate_lead_insights(leads, key_cols)
            
            return {
                "success": True,
                "leads": leads,
                "count": len(leads),
                "sql_used": sql,
                "insights": insights,
                "key_columns": key_cols
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "sql_attempted": sql
            }

    def _build_fallback_query(self, key_cols: Dict, limit: int) -> str:
        """Build a basic lead query without AI"""
        conditions = []
        
        if key_cols.get('email'):
            conditions.append(f'"{key_cols["email"]}" IS NOT NULL AND "{key_cols["email"]}" != ""')
        
        if key_cols.get('phone'):
            conditions.append(f'"{key_cols["phone"]}" IS NOT NULL AND "{key_cols["phone"]}" != ""')
        
        where_clause = " OR ".join(conditions) if conditions else "1=1"
        
        return f'SELECT * FROM "{self.table_name}" WHERE ({where_clause}) LIMIT {limit}'

    def _generate_lead_insights(self, leads: List[Dict], key_cols: Dict) -> List[Dict]:
        """Generate insights about the lead set"""
        insights = []
        
        if not leads:
            return insights
        
        total = len(leads)
        
        # Email availability
        if key_cols.get('email'):
            email_col = key_cols['email']
            with_email = sum(1 for l in leads if l.get(email_col))
            insights.append({
                "type": "contact",
                "title": "Email Marketing Ready",
                "description": f"{with_email} of {total} leads have email addresses",
                "percentage": round(with_email / total * 100, 1),
                "icon": "📧"
            })
        
        # Phone availability
        if key_cols.get('phone'):
            phone_col = key_cols['phone']
            with_phone = sum(1 for l in leads if l.get(phone_col))
            insights.append({
                "type": "contact",
                "title": "Phone Outreach Ready",
                "description": f"{with_phone} of {total} leads have phone numbers",
                "percentage": round(with_phone / total * 100, 1),
                "icon": "📞"
            })
        
        # Income analysis if available
        if key_cols.get('income'):
            income_col = key_cols['income']
            incomes = [l.get(income_col) for l in leads if l.get(income_col)]
            if incomes:
                try:
                    numeric_incomes = [float(str(i).replace(',', '').replace('$', '')) for i in incomes if i]
                    if numeric_incomes:
                        avg_income = sum(numeric_incomes) / len(numeric_incomes)
                        insights.append({
                            "type": "demographic",
                            "title": "Income Profile",
                            "description": f"Average income: ${avg_income:,.0f}",
                            "icon": "💰"
                        })
                except:
                    pass
        
        # Quick action suggestions
        insights.append({
            "type": "action",
            "title": "Recommended Action",
            "description": "Send personalized ACA enrollment email to qualified leads",
            "icon": "🎯"
        })
        
        return insights

    def ask_lead_question(self, question: str) -> Dict[str, Any]:
        """
        Ask a natural language question about leads
        
        Examples:
        - "Which leads are best for ACA enrollment?"
        - "Find 50 people I can email about health insurance"
        - "Show me leads without insurance"
        """
        if not self.gemini_model:
            return {"error": "Gemini API not configured"}
        
        columns = [col['name'] for col in self.schema]
        key_cols = self._identify_key_columns()
        
        prompt = f"""You are an ACA enrollment sales expert analyzing a database of potential leads.

Database schema columns: {', '.join(columns)}
Table name: "{self.table_name}"

Key columns identified:
{json.dumps(key_cols, indent=2)}

User question: "{question}"

Generate a response with:
1. A SQLite query to answer their question (if applicable)
2. Explanation of your approach
3. Recommended actions

Return JSON with:
{{
    "sql": "the SQL query",
    "explanation": "why this query answers their question",
    "recommended_actions": ["action1", "action2"],
    "marketing_tip": "a tip for converting these leads"
}}
"""
        
        try:
            response = self.gemini_model.generate_content(prompt)
            text = response.text.strip()
            
            # Parse JSON from response
            if '```json' in text:
                text = text.split('```json')[1].split('```')[0]
            elif '```' in text:
                text = text.split('```')[1].split('```')[0]
            
            result = json.loads(text)
            
            # Execute the query
            if result.get('sql'):
                conn = sqlite3.connect(self.db_path)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                sql = result['sql']
                if not sql.upper().strip().startswith('SELECT'):
                    return {"error": "Only SELECT queries allowed"}
                
                cursor.execute(sql)
                rows = cursor.fetchall()
                conn.close()
                
                result['leads'] = [dict(row) for row in rows]
                result['count'] = len(rows)
            
            return result
            
        except Exception as e:
            return {"error": str(e)}

    def generate_marketing_list(
        self,
        criteria: str,
        limit: int = 100,
        require_email: bool = True
    ) -> Dict[str, Any]:
        """
        Generate a marketing list based on criteria
        
        Args:
            criteria: What type of leads (e.g., "ACA eligible", "no insurance")
            limit: Maximum leads to return
            require_email: Only include leads with email addresses
        """
        key_cols = self._identify_key_columns()
        
        conditions = []
        if require_email and key_cols.get('email'):
            conditions.append(f'"{key_cols["email"]}" IS NOT NULL AND "{key_cols["email"]}" != ""')
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        # AI-enhanced query if available
        if self.gemini_model:
            columns = [col['name'] for col in self.schema]
            prompt = f"""Generate a SQL WHERE clause to find leads matching: "{criteria}"

Available columns: {', '.join(columns)}
Table name: "{self.table_name}"

Current conditions: {where_clause}

Return ONLY the additional WHERE conditions (without WHERE keyword).
If no specific conditions apply, return "1=1".
"""
            try:
                response = self.gemini_model.generate_content(prompt)
                ai_conditions = response.text.strip()
                if ai_conditions and ai_conditions != "1=1":
                    where_clause = f"({where_clause}) AND ({ai_conditions})"
            except:
                pass
        
        sql = f'SELECT * FROM "{self.table_name}" WHERE {where_clause} LIMIT {limit}'
        
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            conn.close()
            
            leads = [dict(row) for row in rows]
            
            return {
                "success": True,
                "leads": leads,
                "count": len(leads),
                "criteria": criteria,
                "sql_used": sql,
                "email_column": key_cols.get('email'),
                "ready_for_email": require_email and key_cols.get('email') is not None
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}


def get_lead_service(db_path: str, table_name: str = "converted_data") -> LeadScoringService:
    """Factory function to create LeadScoringService"""
    return LeadScoringService(db_path, table_name)
