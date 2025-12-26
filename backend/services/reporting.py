"""
Reporting Service
Generate PDF and Excel reports from populations
"""

import json
import os
import uuid
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict
from datetime import datetime
from io import BytesIO
import csv


@dataclass
class Report:
    """Represents a generated report"""
    id: str
    name: str
    type: str  # summary, detailed, comparison
    population_ids: List[str]
    created_at: str
    format: str  # pdf, excel, csv
    file_path: Optional[str] = None
    data: Dict = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        return asdict(self)


class ReportGenerator:
    """Generate reports from population data"""
    
    def __init__(self, population_manager):
        self.population_manager = population_manager
        self.reports_dir = "reports"
        os.makedirs(self.reports_dir, exist_ok=True)
    
    def generate_summary_report(
        self,
        pop_id: str,
        name: str = None
    ) -> Report:
        """Generate a summary statistics report for a population"""
        stats = self.population_manager.get_population_stats(pop_id)
        pop = self.population_manager.get_population(pop_id)
        
        if not pop:
            raise ValueError("Population not found")
        
        report_id = str(uuid.uuid4())[:12]
        report_name = name or f"Summary Report - {pop.name}"
        
        report = Report(
            id=report_id,
            name=report_name,
            type="summary",
            population_ids=[pop_id],
            created_at=datetime.now().isoformat(),
            format="json",
            data={
                "population": pop.to_dict(),
                "statistics": stats,
                "generated_at": datetime.now().isoformat()
            }
        )
        
        return report
    
    def generate_detailed_report(
        self,
        pop_id: str,
        columns: List[str] = None,
        limit: int = 10000,
        name: str = None
    ) -> Report:
        """Generate a detailed data report with all records"""
        pop_data = self.population_manager.get_population_data(pop_id, limit=limit)
        pop = self.population_manager.get_population(pop_id)
        
        if not pop:
            raise ValueError("Population not found")
        
        # Filter columns if specified
        if columns and pop_data.get("records"):
            pop_data["records"] = [
                {k: v for k, v in record.items() if k in columns}
                for record in pop_data["records"]
            ]
            pop_data["columns"] = columns
        
        report_id = str(uuid.uuid4())[:12]
        report_name = name or f"Detailed Report - {pop.name}"
        
        report = Report(
            id=report_id,
            name=report_name,
            type="detailed",
            population_ids=[pop_id],
            created_at=datetime.now().isoformat(),
            format="json",
            data=pop_data
        )
        
        return report
    
    def generate_comparison_report(
        self,
        pop_ids: List[str],
        name: str = None
    ) -> Report:
        """Compare multiple populations"""
        populations = []
        for pop_id in pop_ids:
            pop = self.population_manager.get_population(pop_id)
            if pop:
                stats = self.population_manager.get_population_stats(pop_id)
                populations.append({
                    "population": pop.to_dict(),
                    "statistics": stats
                })
        
        report_id = str(uuid.uuid4())[:12]
        report_name = name or f"Comparison Report - {len(pop_ids)} populations"
        
        # Calculate comparison metrics
        comparison = {
            "populations": populations,
            "summary": {
                "total_populations": len(populations),
                "combined_records": sum(p["population"]["count"] for p in populations),
                "largest": max(populations, key=lambda x: x["population"]["count"])["population"]["name"] if populations else None,
                "smallest": min(populations, key=lambda x: x["population"]["count"])["population"]["name"] if populations else None
            }
        }
        
        report = Report(
            id=report_id,
            name=report_name,
            type="comparison",
            population_ids=pop_ids,
            created_at=datetime.now().isoformat(),
            format="json",
            data=comparison
        )
        
        return report
    
    def export_to_csv(self, report: Report) -> str:
        """Export report data to CSV file"""
        filepath = os.path.join(self.reports_dir, f"{report.id}.csv")
        
        if report.type == "detailed":
            records = report.data.get("records", [])
            columns = report.data.get("columns", [])
            
            with open(filepath, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=columns)
                writer.writeheader()
                writer.writerows(records)
        else:
            # For summary/comparison, export as key-value
            with open(filepath, 'w', newline='') as f:
                writer = csv.writer(f)
                self._write_dict_to_csv(writer, report.data)
        
        report.file_path = filepath
        report.format = "csv"
        return filepath
    
    def _write_dict_to_csv(self, writer, data, prefix=""):
        """Recursively write dict to CSV"""
        for key, value in data.items():
            full_key = f"{prefix}.{key}" if prefix else key
            if isinstance(value, dict):
                self._write_dict_to_csv(writer, value, full_key)
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        self._write_dict_to_csv(writer, item, f"{full_key}[{i}]")
                    else:
                        writer.writerow([f"{full_key}[{i}]", str(item)])
            else:
                writer.writerow([full_key, str(value)])
    
    def export_to_json(self, report: Report) -> str:
        """Export report to JSON file"""
        filepath = os.path.join(self.reports_dir, f"{report.id}.json")
        
        with open(filepath, 'w') as f:
            json.dump(report.to_dict(), f, indent=2, default=str)
        
        report.file_path = filepath
        report.format = "json"
        return filepath
    
    def generate_html_report(self, report: Report) -> str:
        """Generate HTML report for preview"""
        pop = report.data.get("population", {})
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>{report.name}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }}
                .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
                h1 {{ color: #333; border-bottom: 2px solid #6366f1; padding-bottom: 10px; }}
                h2 {{ color: #555; margin-top: 30px; }}
                .stat-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }}
                .stat-card {{ background: #f8fafc; padding: 20px; border-radius: 8px; border-left: 4px solid #6366f1; }}
                .stat-label {{ color: #666; font-size: 0.875rem; }}
                .stat-value {{ font-size: 1.5rem; font-weight: bold; color: #333; }}
                table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
                th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
                th {{ background: #6366f1; color: white; }}
                tr:hover {{ background: #f5f5f5; }}
                .footer {{ margin-top: 40px; padding-top: 20px; border-top: 1px solid #ddd; color: #666; font-size: 0.875rem; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>{report.name}</h1>
                <p>Generated: {report.created_at}</p>
        """
        
        if report.type == "summary":
            stats = report.data.get("statistics", {})
            html += f"""
                <h2>Population: {pop.get('name', 'Unknown')}</h2>
                <div class="stat-grid">
                    <div class="stat-card">
                        <div class="stat-label">Total Records</div>
                        <div class="stat-value">{pop.get('count', 0):,}</div>
                    </div>
                </div>
                <h2>Column Statistics</h2>
                <table>
                    <tr><th>Column</th><th>Type</th><th>Unique</th><th>Nulls</th><th>Min</th><th>Max</th><th>Avg</th></tr>
            """
            for col_name, col_stats in stats.get("columns", {}).items():
                html += f"""
                    <tr>
                        <td>{col_name}</td>
                        <td>{col_stats.get('type', '-')}</td>
                        <td>{col_stats.get('unique_count', '-')}</td>
                        <td>{col_stats.get('null_count', '-')}</td>
                        <td>{col_stats.get('min', '-')}</td>
                        <td>{col_stats.get('max', '-')}</td>
                        <td>{col_stats.get('avg', '-'):.2f if col_stats.get('avg') else '-'}</td>
                    </tr>
                """
            html += "</table>"
        
        elif report.type == "detailed":
            records = report.data.get("records", [])[:100]  # Limit for HTML
            columns = report.data.get("columns", [])
            
            html += f"""
                <h2>Population: {pop.get('name', 'Unknown')}</h2>
                <p>Showing {len(records)} of {report.data.get('total', 0):,} records</p>
                <table>
                    <tr>
            """
            for col in columns:
                html += f"<th>{col}</th>"
            html += "</tr>"
            for record in records:
                html += "<tr>"
                for col in columns:
                    val = record.get(col, '')
                    html += f"<td>{val}</td>"
                html += "</tr>"
            html += "</table>"
        
        elif report.type == "comparison":
            html += "<h2>Population Comparison</h2>"
            summary = report.data.get("summary", {})
            html += f"""
                <div class="stat-grid">
                    <div class="stat-card">
                        <div class="stat-label">Populations Compared</div>
                        <div class="stat-value">{summary.get('total_populations', 0)}</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">Combined Records</div>
                        <div class="stat-value">{summary.get('combined_records', 0):,}</div>
                    </div>
                </div>
                <table>
                    <tr><th>Population</th><th>Records</th><th>Created</th></tr>
            """
            for p in report.data.get("populations", []):
                pop_info = p.get("population", {})
                html += f"""
                    <tr>
                        <td>{pop_info.get('name', 'Unknown')}</td>
                        <td>{pop_info.get('count', 0):,}</td>
                        <td>{pop_info.get('created_at', '-')}</td>
                    </tr>
                """
            html += "</table>"
        
        html += """
                <div class="footer">
                    <p>Report generated by YXDB Converter</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        return html
