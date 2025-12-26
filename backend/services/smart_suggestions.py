"""
Smart Query Suggestions
Provides AI-powered query suggestions based on schema analysis
"""

from typing import List, Dict, Any
import os


class SmartQuerySuggestions:
    """Generate intelligent query suggestions based on data schema"""
    
    def __init__(self, schema: List[Dict], table_name: str = "converted_data"):
        self.schema = schema
        self.table_name = table_name
        self._init_gemini()
    
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
    
    def analyze_schema(self) -> Dict[str, Any]:
        """Analyze schema to understand data types and patterns"""
        analysis = {
            'columns': [],
            'numeric_columns': [],
            'text_columns': [],
            'date_columns': [],
            'id_columns': [],
            'contact_columns': [],
            'geo_columns': [],
            'amount_columns': []
        }
        
        for col in self.schema:
            name = col.get('name', '')
            sql_type = col.get('sql_type', 'TEXT')
            name_lower = name.lower()
            
            analysis['columns'].append(name)
            
            # Categorize by type
            if sql_type in ['INTEGER', 'REAL', 'NUMERIC']:
                analysis['numeric_columns'].append(name)
            else:
                analysis['text_columns'].append(name)
            
            # Detect special columns
            if any(x in name_lower for x in ['date', 'time', 'created', 'updated']):
                analysis['date_columns'].append(name)
            
            if any(x in name_lower for x in ['id', 'key', 'code']):
                analysis['id_columns'].append(name)
            
            if any(x in name_lower for x in ['email', 'phone', 'mobile', 'contact']):
                analysis['contact_columns'].append(name)
            
            if any(x in name_lower for x in ['region', 'state', 'city', 'country', 'zip', 'address']):
                analysis['geo_columns'].append(name)
            
            if any(x in name_lower for x in ['amount', 'price', 'cost', 'revenue', 'sales', 'total']):
                analysis['amount_columns'].append(name)
        
        return analysis
    
    def generate_suggestions(self) -> List[Dict[str, str]]:
        """Generate smart query suggestions based on schema"""
        analysis = self.analyze_schema()
        suggestions = []
        
        # Basic suggestions
        suggestions.append({
            'category': 'overview',
            'icon': '📊',
            'title': 'Data Overview',
            'natural_language': 'Show me a summary of the data including total count',
            'description': f'Get overview of {len(analysis["columns"])} columns'
        })
        
        # Numeric analysis
        if analysis['numeric_columns']:
            col = analysis['numeric_columns'][0]
            suggestions.append({
                'category': 'analysis',
                'icon': '📈',
                'title': f'Analyze {col}',
                'natural_language': f'Show me the average, min, max, and count of {col}',
                'description': f'Statistical analysis of {col}'
            })
            
            if len(analysis['numeric_columns']) > 1:
                col2 = analysis['numeric_columns'][1]
                suggestions.append({
                    'category': 'analysis',
                    'icon': '🔗',
                    'title': 'Compare Metrics',
                    'natural_language': f'Compare {col} and {col2} across all records',
                    'description': 'Cross-metric analysis'
                })
        
        # Amount/Revenue analysis
        if analysis['amount_columns']:
            col = analysis['amount_columns'][0]
            suggestions.append({
                'category': 'financial',
                'icon': '💰',
                'title': 'Revenue Analysis',
                'natural_language': f'Show total {col} and average {col}',
                'description': f'Financial summary of {col}'
            })
            
            suggestions.append({
                'category': 'financial',
                'icon': '🏆',
                'title': 'Top Performers',
                'natural_language': f'Show top 10 records by {col} in descending order',
                'description': f'Highest {col} records'
            })
        
        # Date-based analysis
        if analysis['date_columns']:
            col = analysis['date_columns'][0]
            suggestions.append({
                'category': 'trends',
                'icon': '📅',
                'title': 'Time Trends',
                'natural_language': f'Show record counts grouped by {col}',
                'description': 'Analyze patterns over time'
            })
        
        # Geographic analysis
        if analysis['geo_columns']:
            col = analysis['geo_columns'][0]
            suggestions.append({
                'category': 'geographic',
                'icon': '🗺️',
                'title': 'Geographic Distribution',
                'natural_language': f'Show record counts by {col}',
                'description': f'Regional breakdown by {col}'
            })
        
        # Contact list
        if analysis['contact_columns']:
            col = analysis['contact_columns'][0]
            suggestions.append({
                'category': 'contacts',
                'icon': '📧',
                'title': 'Contact Extraction',
                'natural_language': f'Show all records with {col} filled in',
                'description': 'Extract valid contacts for outreach'
            })
        
        # Data quality
        suggestions.append({
            'category': 'quality',
            'icon': '✅',
            'title': 'Data Quality Check',
            'natural_language': 'Find records that have null or missing values',
            'description': 'Identify incomplete records'
        })
        
        # Unique values
        if analysis['text_columns']:
            col = analysis['text_columns'][0]
            suggestions.append({
                'category': 'exploration',
                'icon': '🔍',
                'title': f'Unique {col}',
                'natural_language': f'Show distinct values of {col} with counts',
                'description': 'Categorical breakdown'
            })
        
        return suggestions
    
    def get_alternative_queries(self, original_query: str, error_message: str = None) -> List[Dict[str, str]]:
        """Generate alternative query suggestions when a query fails or returns no results"""
        analysis = self.analyze_schema()
        alternatives = []
        
        # Use Gemini if available
        if self.gemini_model:
            try:
                prompt = f"""Given this failed or empty query: "{original_query}"
{f'Error: {error_message}' if error_message else 'Query returned no results'}

Database schema columns: {', '.join(analysis['columns'])}

Suggest 3 alternative natural language queries that might work better.
Format each as a JSON object with 'title' and 'query' keys.
Return only a JSON array, no other text."""

                response = self.gemini_model.generate_content(prompt)
                import json
                alt_queries = json.loads(response.text.strip())
                
                for alt in alt_queries[:3]:
                    alternatives.append({
                        'title': alt.get('title', 'Alternative Query'),
                        'query': alt.get('query', '')
                    })
                    
            except Exception as e:
                pass
        
        # Fallback suggestions
        if not alternatives:
            alternatives = [
                {
                    'title': 'Show All Data',
                    'query': 'Show me all records limited to 100'
                },
                {
                    'title': 'Count Records',
                    'query': 'How many total records are there?'
                },
                {
                    'title': 'Sample Data',
                    'query': 'Show me 10 random sample records'
                }
            ]
        
        return alternatives
    
    def refine_query(self, query: str) -> Dict[str, Any]:
        """Use AI to refine and improve a query"""
        if not self.gemini_model:
            return {'refined': query, 'improvements': []}
        
        analysis = self.analyze_schema()
        
        try:
            prompt = f"""Given this natural language query for a database: "{query}"

Available columns: {', '.join(analysis['columns'])}

Please:
1. Rephrase the query to be more specific and SQL-friendly
2. Suggest any improvements

Return JSON with:
- "refined": the improved query
- "improvements": list of improvement suggestions
- "sql_hint": what the SQL might look like"""

            response = self.gemini_model.generate_content(prompt)
            import json
            
            # Clean response
            text = response.text.strip()
            if text.startswith('```'):
                text = text.split('```')[1]
                if text.startswith('json'):
                    text = text[4:]
            
            result = json.loads(text)
            return result
            
        except Exception as e:
            return {
                'refined': query,
                'improvements': [],
                'error': str(e)
            }
