import { useState, useEffect } from 'react';
import { useToast } from './Toast';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

/**
 * AI Lead Finder Component
 * Uses Gemini AI to identify and analyze best leads for ACA enrollment
 */
export default function LeadFinder({ jobId, schema, onSavePopulation }) {
    const [loading, setLoading] = useState(false);
    const [leads, setLeads] = useState([]);
    const [insights, setInsights] = useState([]);
    const [question, setQuestion] = useState('');
    const [selectedLeads, setSelectedLeads] = useState(new Set());
    const [showEmailModal, setShowEmailModal] = useState(false);
    const [emailConfig, setEmailConfig] = useState({
        subject: 'Your ACA Health Insurance Options',
        enrollmentLink: 'https://www.healthcare.gov/',
        customMessage: ''
    });
    const [sendingEmail, setSendingEmail] = useState(false);
    const { addToast } = useToast();

    // Quick action suggestions
    const quickActions = [
        {
            icon: '🎯',
            label: 'Best 100 Leads',
            question: 'Find the top 100 best leads for ACA enrollment'
        },
        {
            icon: '📧',
            label: 'Email Ready Leads',
            question: 'Give me 100 leads with valid email addresses for direct marketing'
        },
        {
            icon: '🏆',
            label: 'High Value Leads',
            question: 'Find leads most likely to qualify for ACA subsidies'
        },
        {
            icon: '📞',
            label: 'Phone Outreach',
            question: 'Find leads with phone numbers for outbound calling'
        },
        {
            icon: '🗺️',
            label: 'By Region',
            question: 'Show me leads grouped by state or region'
        },
        {
            icon: '⚡',
            label: 'Quick Wins',
            question: 'Find leads that are easiest to convert for ACA enrollment'
        }
    ];

    const handleQuickAction = (action) => {
        setQuestion(action.question);
        handleAskQuestion(action.question);
    };

    const handleAskQuestion = async (q = question) => {
        if (!q.trim()) return;

        setLoading(true);
        try {
            const response = await fetch(`${API_URL}/ai/leads/ask`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    job_id: jobId,
                    question: q
                })
            });

            const data = await response.json();

            if (data.error) {
                addToast(data.error, 'error');
            } else {
                setLeads(data.leads || []);
                if (data.recommended_actions) {
                    setInsights(data.recommended_actions.map((a, i) => ({
                        type: 'action',
                        title: `Recommendation ${i + 1}`,
                        description: a,
                        icon: '💡'
                    })));
                }
                if (data.marketing_tip) {
                    setInsights(prev => [...prev, {
                        type: 'tip',
                        title: 'Marketing Tip',
                        description: data.marketing_tip,
                        icon: '🎯'
                    }]);
                }
                addToast(`Found ${data.count || 0} leads`, 'success');
            }
        } catch (err) {
            addToast('Failed to query leads', 'error');
        } finally {
            setLoading(false);
        }
    };

    const handleGetBestLeads = async () => {
        setLoading(true);
        try {
            const response = await fetch(`${API_URL}/ai/leads/best`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    job_id: jobId,
                    limit: 100,
                    criteria: 'ACA enrollment'
                })
            });

            const data = await response.json();

            if (data.success) {
                setLeads(data.leads || []);
                setInsights(data.insights || []);
                addToast(`Found ${data.count} best leads for ACA enrollment`, 'success');
            } else {
                addToast(data.error || 'Failed to get leads', 'error');
            }
        } catch (err) {
            addToast('Failed to get leads', 'error');
        } finally {
            setLoading(false);
        }
    };

    const toggleLeadSelection = (rowId) => {
        const newSelected = new Set(selectedLeads);
        if (newSelected.has(rowId)) {
            newSelected.delete(rowId);
        } else {
            newSelected.add(rowId);
        }
        setSelectedLeads(newSelected);
    };

    const selectAllLeads = () => {
        if (selectedLeads.size === leads.length) {
            setSelectedLeads(new Set());
        } else {
            setSelectedLeads(new Set(leads.map(l => l._row_id)));
        }
    };

    const handleSendEmails = async () => {
        if (selectedLeads.size === 0) {
            addToast('Please select leads first', 'warning');
            return;
        }

        setSendingEmail(true);
        try {
            const response = await fetch(`${API_URL}/ai/leads/send-email`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    job_id: jobId,
                    lead_ids: Array.from(selectedLeads),
                    subject: emailConfig.subject,
                    enrollment_link: emailConfig.enrollmentLink,
                    custom_message: emailConfig.customMessage
                })
            });

            const data = await response.json();

            if (data.success) {
                addToast(`Sent ${data.emails_sent} emails successfully`, 'success');
                setShowEmailModal(false);
                setSelectedLeads(new Set());
            } else {
                addToast(data.error || 'Failed to send emails', 'error');
            }
        } catch (err) {
            addToast('Failed to send emails', 'error');
        } finally {
            setSendingEmail(false);
        }
    };

    const handleSaveAsPopulation = async () => {
        if (leads.length === 0) return;

        const name = prompt('Enter population name:', `ACA Leads - ${new Date().toLocaleDateString()}`);
        if (!name) return;

        // Save leads as population
        if (onSavePopulation) {
            onSavePopulation(name, leads);
            addToast(`Saved ${leads.length} leads as "${name}"`, 'success');
        }
    };

    // Get column headers from leads
    const columns = leads.length > 0
        ? Object.keys(leads[0]).filter(k => !k.startsWith('_'))
        : [];

    return (
        <div className="lead-finder">
            {/* Header */}
            <div className="lead-finder-header">
                <div className="header-left">
                    <h2>🎯 AI Lead Finder</h2>
                    <span className="gemini-badge">Powered by Gemini</span>
                </div>
                {leads.length > 0 && (
                    <div className="header-actions">
                        <button
                            className="btn btn-secondary"
                            onClick={handleSaveAsPopulation}
                        >
                            💾 Save as Population
                        </button>
                        <button
                            className="btn btn-primary"
                            onClick={() => setShowEmailModal(true)}
                            disabled={selectedLeads.size === 0}
                        >
                            📧 Send Marketing Email ({selectedLeads.size})
                        </button>
                    </div>
                )}
            </div>

            {/* Quick Actions */}
            <div className="quick-actions">
                <h3>Quick Actions</h3>
                <div className="actions-grid">
                    {quickActions.map((action, i) => (
                        <button
                            key={i}
                            className="quick-action-btn"
                            onClick={() => handleQuickAction(action)}
                            disabled={loading}
                        >
                            <span className="action-icon">{action.icon}</span>
                            <span className="action-label">{action.label}</span>
                        </button>
                    ))}
                </div>
            </div>

            {/* Question Input */}
            <div className="question-input">
                <div className="input-group">
                    <input
                        type="text"
                        placeholder="Ask about your leads... e.g., 'Find 50 leads with email addresses in Texas'"
                        value={question}
                        onChange={(e) => setQuestion(e.target.value)}
                        onKeyDown={(e) => e.key === 'Enter' && handleAskQuestion()}
                    />
                    <button
                        className="btn btn-primary"
                        onClick={() => handleAskQuestion()}
                        disabled={loading || !question.trim()}
                    >
                        {loading ? '🔄' : '✨'} Ask AI
                    </button>
                </div>
            </div>

            {/* Insights */}
            {insights.length > 0 && (
                <div className="lead-insights">
                    <h3>📊 Insights</h3>
                    <div className="insights-list">
                        {insights.map((insight, i) => (
                            <div key={i} className={`insight-item insight-${insight.type}`}>
                                <span className="insight-icon">{insight.icon}</span>
                                <div className="insight-content">
                                    <strong>{insight.title}</strong>
                                    <p>{insight.description}</p>
                                    {insight.percentage !== undefined && (
                                        <div className="insight-bar">
                                            <div
                                                className="insight-bar-fill"
                                                style={{ width: `${insight.percentage}%` }}
                                            />
                                        </div>
                                    )}
                                </div>
                            </div>
                        ))}
                    </div>
                </div>
            )}

            {/* Results Table */}
            {leads.length > 0 && (
                <div className="leads-table-container">
                    <div className="table-header">
                        <h3>📋 Lead Results ({leads.length})</h3>
                        <button className="btn btn-sm" onClick={selectAllLeads}>
                            {selectedLeads.size === leads.length ? 'Deselect All' : 'Select All'}
                        </button>
                    </div>

                    <div className="table-scroll">
                        <table className="leads-table">
                            <thead>
                                <tr>
                                    <th>
                                        <input
                                            type="checkbox"
                                            checked={selectedLeads.size === leads.length}
                                            onChange={selectAllLeads}
                                        />
                                    </th>
                                    {columns.slice(0, 8).map(col => (
                                        <th key={col}>{col}</th>
                                    ))}
                                </tr>
                            </thead>
                            <tbody>
                                {leads.slice(0, 100).map((lead, i) => (
                                    <tr
                                        key={lead._row_id || i}
                                        className={selectedLeads.has(lead._row_id) ? 'selected' : ''}
                                    >
                                        <td>
                                            <input
                                                type="checkbox"
                                                checked={selectedLeads.has(lead._row_id)}
                                                onChange={() => toggleLeadSelection(lead._row_id)}
                                            />
                                        </td>
                                        {columns.slice(0, 8).map(col => (
                                            <td key={col}>{String(lead[col] || '')}</td>
                                        ))}
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                </div>
            )}

            {/* Empty State */}
            {!loading && leads.length === 0 && (
                <div className="empty-state">
                    <p>🔍 Use the quick actions above or ask a question to find leads</p>
                    <button className="btn btn-primary" onClick={handleGetBestLeads}>
                        ✨ Get Best 100 Leads for ACA Enrollment
                    </button>
                </div>
            )}

            {/* Loading State */}
            {loading && (
                <div className="loading-state">
                    <div className="gemini-loader">
                        <span></span><span></span><span></span><span></span>
                    </div>
                    <p>AI is analyzing your data...</p>
                </div>
            )}

            {/* Email Modal */}
            {showEmailModal && (
                <div className="modal-overlay" onClick={() => setShowEmailModal(false)}>
                    <div className="modal email-modal" onClick={e => e.stopPropagation()}>
                        <div className="modal-header">
                            <h3>📧 Send Marketing Email</h3>
                            <button className="close-btn" onClick={() => setShowEmailModal(false)}>×</button>
                        </div>

                        <div className="modal-body">
                            <p className="modal-info">
                                Sending to <strong>{selectedLeads.size}</strong> selected leads
                            </p>

                            <div className="form-group">
                                <label>Email Subject</label>
                                <input
                                    type="text"
                                    value={emailConfig.subject}
                                    onChange={(e) => setEmailConfig({ ...emailConfig, subject: e.target.value })}
                                />
                            </div>

                            <div className="form-group">
                                <label>Enrollment Link</label>
                                <input
                                    type="url"
                                    value={emailConfig.enrollmentLink}
                                    onChange={(e) => setEmailConfig({ ...emailConfig, enrollmentLink: e.target.value })}
                                    placeholder="https://www.healthcare.gov/"
                                />
                            </div>

                            <div className="form-group">
                                <label>Custom Message (optional)</label>
                                <textarea
                                    value={emailConfig.customMessage}
                                    onChange={(e) => setEmailConfig({ ...emailConfig, customMessage: e.target.value })}
                                    placeholder="Add a personalized message..."
                                    rows={4}
                                />
                            </div>
                        </div>

                        <div className="modal-footer">
                            <button className="btn btn-secondary" onClick={() => setShowEmailModal(false)}>
                                Cancel
                            </button>
                            <button
                                className="btn btn-primary"
                                onClick={handleSendEmails}
                                disabled={sendingEmail}
                            >
                                {sendingEmail ? 'Sending...' : `Send to ${selectedLeads.size} Leads`}
                            </button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}
