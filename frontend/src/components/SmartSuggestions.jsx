import { useState, useEffect } from 'react';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

/**
 * Smart Suggestions Component
 * Powered by Gemini AI - suggests prompts and analysis based on uploaded data
 */
export default function SmartSuggestions({ jobId, schema, onSuggestionClick }) {
    const [suggestions, setSuggestions] = useState([]);
    const [loading, setLoading] = useState(false);

    // Default suggestions when no data is loaded
    const defaultSuggestions = [
        {
            icon: '📊',
            title: 'Analyze Customer Segments',
            description: 'Upload customer data to discover high-value segments and patterns',
            prompt: 'Identify and analyze the top 10 customer segments by value'
        },
        {
            icon: '📈',
            title: 'Find Growth Trends',
            description: 'Detect trends and opportunities in your sales data',
            prompt: 'Show me month-over-month growth trends'
        },
        {
            icon: '🎯',
            title: 'Target Audience Builder',
            description: 'Create precise audience segments for marketing campaigns',
            prompt: 'Find customers who purchased in the last 30 days but not in the last 7'
        },
        {
            icon: '⚠️',
            title: 'Anomaly Detection',
            description: 'AI identifies unusual patterns and outliers in your data',
            prompt: 'Find records with unusual or outlier values'
        },
        {
            icon: '🔗',
            title: 'Data Quality Check',
            description: 'Analyze completeness and consistency of your data',
            prompt: 'Show me columns with missing or null values'
        },
        {
            icon: '💡',
            title: 'Custom Analysis',
            description: 'Ask any question about your data in plain English',
            prompt: ''
        }
    ];

    // Generate AI suggestions based on schema
    useEffect(() => {
        if (jobId && schema) {
            generateAISuggestions();
        } else {
            setSuggestions(defaultSuggestions);
        }
    }, [jobId, schema]);

    const generateAISuggestions = async () => {
        setLoading(true);

        // Generate smart suggestions based on schema
        const columnNames = schema.map(s => s.name);
        const hasEmail = columnNames.some(c => c.toLowerCase().includes('email'));
        const hasPhone = columnNames.some(c => c.toLowerCase().includes('phone'));
        const hasDate = columnNames.some(c => c.toLowerCase().includes('date'));
        const hasSales = columnNames.some(c => c.toLowerCase().includes('sales') || c.toLowerCase().includes('amount'));
        const hasName = columnNames.some(c => c.toLowerCase().includes('name'));
        const hasRegion = columnNames.some(c => c.toLowerCase().includes('region') || c.toLowerCase().includes('state') || c.toLowerCase().includes('city'));

        const smartSuggestions = [
            {
                icon: '📊',
                title: 'Quick Data Summary',
                description: `Analyze ${schema.length} columns and find key insights`,
                prompt: 'Give me a summary of this data including record count and key statistics'
            }
        ];

        if (hasSales) {
            smartSuggestions.push({
                icon: '💰',
                title: 'Revenue Analysis',
                description: 'Identify top performers and revenue patterns',
                prompt: 'Show me total sales and average sales, grouped by the most relevant category'
            });
        }

        if (hasDate) {
            smartSuggestions.push({
                icon: '📅',
                title: 'Time-Based Trends',
                description: 'Analyze patterns over time',
                prompt: 'Show me records grouped by date, ordered chronologically'
            });
        }

        if (hasRegion) {
            smartSuggestions.push({
                icon: '🗺️',
                title: 'Geographic Distribution',
                description: 'Analyze data by location',
                prompt: 'Show me record counts grouped by region or location'
            });
        }

        if (hasEmail || hasPhone) {
            smartSuggestions.push({
                icon: '📨',
                title: 'Contact List Ready',
                description: 'Create targeted outreach lists',
                prompt: `Show me records with ${hasEmail ? 'email addresses' : 'phone numbers'} for outreach`
            });
        }

        smartSuggestions.push({
            icon: '🔍',
            title: 'Data Quality Report',
            description: 'Check for missing values and data issues',
            prompt: 'Find records with null or missing values in important fields'
        });

        smartSuggestions.push({
            icon: '💡',
            title: 'Ask Anything',
            description: 'Type any question about your data',
            prompt: ''
        });

        setSuggestions(smartSuggestions);
        setLoading(false);
    };

    const handleClick = (suggestion) => {
        if (onSuggestionClick && suggestion.prompt) {
            onSuggestionClick(suggestion.prompt);
        }
    };

    return (
        <div className="smart-suggestions">
            <div className="smart-suggestions-header">
                <h3>
                    ✨ AI-Powered Analysis
                    <span className="gemini-badge">Gemini</span>
                </h3>
            </div>

            <p className="suggestions-subtitle">
                {jobId
                    ? 'Click a suggestion to analyze your data with AI'
                    : 'Upload data to unlock AI-powered insights and analysis'
                }
            </p>

            <div className="suggestions-grid">
                {suggestions.map((suggestion, index) => (
                    <div
                        key={index}
                        className="suggestion-card"
                        onClick={() => handleClick(suggestion)}
                    >
                        <div className="suggestion-icon">{suggestion.icon}</div>
                        <div className="suggestion-title">{suggestion.title}</div>
                        <div className="suggestion-desc">{suggestion.description}</div>
                    </div>
                ))}
            </div>

            {loading && (
                <div className="suggestions-loading">
                    <span className="spinner"></span>
                    Generating smart suggestions...
                </div>
            )}
        </div>
    );
}
