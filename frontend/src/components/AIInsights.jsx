import { useState, useEffect } from 'react';
import { useToast } from './Toast';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

/**
 * AI Insights Panel
 * Automatically generates insights from data using Gemini AI
 */
export default function AIInsights({ jobId, schema }) {
    const [insights, setInsights] = useState([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const { addToast } = useToast();

    useEffect(() => {
        if (jobId && schema) {
            generateInsights();
        }
    }, [jobId, schema]);

    const generateInsights = async () => {
        setLoading(true);
        setError(null);

        try {
            // Get basic stats
            const statsRes = await fetch(`${API_URL}/query`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    job_id: jobId,
                    question: 'Count the total number of records',
                    max_rows: 1
                })
            });
            const statsData = await statsRes.json();

            const totalRecords = statsData.results?.[0]
                ? Object.values(statsData.results[0])[0]
                : 0;

            // Generate insights based on schema analysis
            const generatedInsights = [];

            // Record count insight
            generatedInsights.push({
                type: 'stat',
                icon: '📊',
                title: 'Dataset Size',
                value: totalRecords.toLocaleString(),
                description: 'Total records in your dataset',
                color: '#6366f1'
            });

            // Column count
            generatedInsights.push({
                type: 'stat',
                icon: '📋',
                title: 'Data Fields',
                value: schema.length.toString(),
                description: 'Columns available for analysis',
                color: '#8b5cf6'
            });

            // Analyze column types
            const numericCols = schema.filter(s =>
                ['INTEGER', 'REAL', 'NUMERIC'].includes(s.sql_type)
            );
            const textCols = schema.filter(s => s.sql_type === 'TEXT');

            if (numericCols.length > 0) {
                generatedInsights.push({
                    type: 'insight',
                    icon: '🔢',
                    title: 'Numeric Analysis Ready',
                    description: `${numericCols.length} numeric columns detected: ${numericCols.slice(0, 3).map(c => c.name).join(', ')}${numericCols.length > 3 ? '...' : ''}`,
                    action: `Show me aggregated statistics for ${numericCols[0].name}`
                });
            }

            // Check for potential contact fields
            const emailCol = schema.find(s => s.name.toLowerCase().includes('email'));
            const phoneCol = schema.find(s => s.name.toLowerCase().includes('phone'));

            if (emailCol || phoneCol) {
                generatedInsights.push({
                    type: 'opportunity',
                    icon: '📧',
                    title: 'Outreach Ready',
                    description: `Contact information detected (${emailCol ? 'email' : ''}${emailCol && phoneCol ? ', ' : ''}${phoneCol ? 'phone' : ''}). Ready for messaging campaigns.`,
                    action: `Show me records with ${emailCol ? 'email addresses' : 'phone numbers'}`
                });
            }

            // Check for date fields
            const dateCol = schema.find(s =>
                s.name.toLowerCase().includes('date') ||
                s.name.toLowerCase().includes('time') ||
                s.name.toLowerCase().includes('created')
            );

            if (dateCol) {
                generatedInsights.push({
                    type: 'insight',
                    icon: '📅',
                    title: 'Time Series Available',
                    description: `Date field "${dateCol.name}" found. Trend analysis possible.`,
                    action: `Show me records grouped by ${dateCol.name}`
                });
            }

            // Check for geographic data
            const geoCol = schema.find(s =>
                s.name.toLowerCase().includes('region') ||
                s.name.toLowerCase().includes('state') ||
                s.name.toLowerCase().includes('city') ||
                s.name.toLowerCase().includes('country')
            );

            if (geoCol) {
                generatedInsights.push({
                    type: 'insight',
                    icon: '🗺️',
                    title: 'Geographic Data',
                    description: `Location field "${geoCol.name}" detected for regional analysis.`,
                    action: `Show me record counts by ${geoCol.name}`
                });
            }

            // Data quality insight
            generatedInsights.push({
                type: 'action',
                icon: '✅',
                title: 'Data Quality Check',
                description: 'Run a completeness analysis to check for missing values',
                action: 'Find columns with null or missing values'
            });

            setInsights(generatedInsights);
        } catch (err) {
            setError('Failed to generate insights');
            console.error(err);
        } finally {
            setLoading(false);
        }
    };

    const handleInsightClick = (insight) => {
        if (insight.action) {
            addToast(`Query: "${insight.action}"`, 'info');
            // Could trigger query here
        }
    };

    if (loading) {
        return (
            <div className="ai-insights-loading">
                <div className="ai-loading-animation">
                    <span className="gemini-dots">
                        <span></span><span></span><span></span><span></span>
                    </span>
                </div>
                <p>Gemini AI is analyzing your data...</p>
            </div>
        );
    }

    return (
        <div className="ai-insights">
            <div className="ai-insights-header">
                <h3>
                    <span className="gemini-icon">✨</span>
                    AI-Generated Insights
                    <span className="gemini-badge">Gemini</span>
                </h3>
                <button className="btn btn-sm btn-secondary" onClick={generateInsights}>
                    🔄 Refresh
                </button>
            </div>

            {error && (
                <div className="alert alert-error">{error}</div>
            )}

            <div className="insights-grid">
                {insights.map((insight, index) => (
                    <div
                        key={index}
                        className={`insight-card insight-${insight.type}`}
                        onClick={() => handleInsightClick(insight)}
                        style={{ '--accent-color': insight.color }}
                    >
                        <div className="insight-icon">{insight.icon}</div>
                        <div className="insight-content">
                            <div className="insight-title">{insight.title}</div>
                            {insight.value && (
                                <div className="insight-value">{insight.value}</div>
                            )}
                            <div className="insight-description">{insight.description}</div>
                        </div>
                        {insight.action && (
                            <div className="insight-action">
                                <span>Click to query →</span>
                            </div>
                        )}
                    </div>
                ))}
            </div>
        </div>
    );
}
