import { useState, useEffect } from 'react';
import { useToast } from './Toast';
import { Modal } from './Modal';
import { Spinner } from './Loading';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

export default function MessageCenter({ jobId, schema, selectedPopulation }) {
    const [templates, setTemplates] = useState([]);
    const [loading, setLoading] = useState(true);
    const [selectedTemplate, setSelectedTemplate] = useState(null);
    const [showEditor, setShowEditor] = useState(false);
    const [showPreview, setShowPreview] = useState(false);
    const [sendHistory, setSendHistory] = useState([]);
    const [previewData, setPreviewData] = useState(null);
    const { addToast } = useToast();

    useEffect(() => {
        if (jobId) {
            loadTemplates();
            loadSendHistory();
        }
    }, [jobId]);

    const loadTemplates = async () => {
        try {
            const res = await fetch(`${API_URL}/templates?job_id=${jobId}`);
            const data = await res.json();
            setTemplates(data.templates || []);
        } catch (err) {
            addToast('Failed to load templates', 'error');
        } finally {
            setLoading(false);
        }
    };

    const loadSendHistory = async () => {
        try {
            const res = await fetch(`${API_URL}/messaging/history?job_id=${jobId}`);
            const data = await res.json();
            setSendHistory(data.history || []);
        } catch (err) {
            console.error('Failed to load send history');
        }
    };

    const createTemplate = async (template) => {
        try {
            const res = await fetch(`${API_URL}/templates?job_id=${jobId}`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(template)
            });
            const data = await res.json();
            if (data.template) {
                setTemplates([...templates, data.template]);
                addToast('Template created', 'success');
                setShowEditor(false);
            }
        } catch (err) {
            addToast('Failed to create template', 'error');
        }
    };

    const deleteTemplate = async (templateId) => {
        try {
            await fetch(`${API_URL}/template/${templateId}?job_id=${jobId}`, { method: 'DELETE' });
            setTemplates(templates.filter(t => t.id !== templateId));
            addToast('Template deleted', 'success');
        } catch (err) {
            addToast('Failed to delete template', 'error');
        }
    };

    const previewMessage = async (templateId, populationId) => {
        try {
            // Get sample record
            const sampleRes = await fetch(
                `${API_URL}/messaging/sample/${populationId}?job_id=${jobId}`
            );
            const sampleData = await sampleRes.json();

            if (sampleData.sample_record) {
                // Preview with sample
                const previewRes = await fetch(
                    `${API_URL}/template/${templateId}/preview?job_id=${jobId}`,
                    {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            template_id: templateId,
                            sample_record: sampleData.sample_record
                        })
                    }
                );
                const preview = await previewRes.json();
                setPreviewData(preview);
                setShowPreview(true);
            }
        } catch (err) {
            addToast('Failed to preview message', 'error');
        }
    };

    const sendMessages = async (templateId, populationId, dryRun = true) => {
        try {
            const res = await fetch(`${API_URL}/messaging/send?job_id=${jobId}`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    template_id: templateId,
                    population_id: populationId,
                    dry_run: dryRun
                })
            });
            const data = await res.json();

            if (data.send_job) {
                if (dryRun) {
                    addToast(
                        `Dry run complete: Would send to ${data.send_job.sent_count} recipients`,
                        'info'
                    );
                } else {
                    addToast(
                        `Messages sent: ${data.send_job.sent_count} successful, ${data.send_job.failed_count} failed`,
                        data.send_job.failed_count > 0 ? 'warning' : 'success'
                    );
                }
                loadSendHistory();
                setShowPreview(false);
            }
        } catch (err) {
            addToast('Failed to send messages', 'error');
        }
    };

    if (loading) {
        return (
            <div className="card">
                <div className="page-loader">
                    <Spinner size="large" />
                    <p>Loading message center...</p>
                </div>
            </div>
        );
    }

    return (
        <div className="message-center">
            <div className="message-header">
                <h2>📨 Message Center</h2>
                <button className="btn btn-primary" onClick={() => setShowEditor(true)}>
                    ➕ New Template
                </button>
            </div>

            {selectedPopulation && (
                <div className="selected-population-banner">
                    <span>📊 Selected Population: <strong>{selectedPopulation.name}</strong></span>
                    <span>({selectedPopulation.count.toLocaleString()} recipients)</span>
                </div>
            )}

            <div className="message-sections">
                {/* Templates */}
                <section className="message-section">
                    <h3>📝 Message Templates</h3>
                    {templates.length === 0 ? (
                        <p className="empty-text">No templates yet. Create one to get started.</p>
                    ) : (
                        <div className="templates-list">
                            {templates.map(template => (
                                <div key={template.id} className="template-card">
                                    <div className="template-header">
                                        <h4>{template.name}</h4>
                                        <span className="template-channel">
                                            {template.channel === 'email' ? '📧' : '💬'} {template.channel}
                                        </span>
                                    </div>
                                    <p className="template-subject">Subject: {template.subject}</p>
                                    <p className="template-body">{template.body.substring(0, 100)}...</p>
                                    <div className="template-variables">
                                        Variables: {template.variables.map(v => (
                                            <code key={v}>{`{{${v}}}`}</code>
                                        ))}
                                    </div>
                                    <div className="template-actions">
                                        {selectedPopulation && (
                                            <>
                                                <button
                                                    className="btn btn-sm"
                                                    onClick={() => previewMessage(template.id, selectedPopulation.id)}
                                                >
                                                    👁️ Preview
                                                </button>
                                                <button
                                                    className="btn btn-sm btn-primary"
                                                    onClick={() => sendMessages(template.id, selectedPopulation.id, true)}
                                                >
                                                    🧪 Dry Run
                                                </button>
                                            </>
                                        )}
                                        <button
                                            className="btn btn-sm btn-danger"
                                            onClick={() => deleteTemplate(template.id)}
                                        >
                                            🗑️
                                        </button>
                                    </div>
                                </div>
                            ))}
                        </div>
                    )}
                </section>

                {/* Send History */}
                <section className="message-section">
                    <h3>📜 Send History</h3>
                    {sendHistory.length === 0 ? (
                        <p className="empty-text">No messages sent yet.</p>
                    ) : (
                        <div className="history-list">
                            {sendHistory.slice(0, 10).map(job => (
                                <div key={job.id} className={`history-item status-${job.status}`}>
                                    <div className="history-info">
                                        <span className="history-date">
                                            {new Date(job.created_at).toLocaleString()}
                                        </span>
                                        <span className={`history-status ${job.status}`}>
                                            {job.status}
                                        </span>
                                    </div>
                                    <div className="history-stats">
                                        <span>✅ {job.sent_count} sent</span>
                                        {job.failed_count > 0 && (
                                            <span className="failed">❌ {job.failed_count} failed</span>
                                        )}
                                    </div>
                                </div>
                            ))}
                        </div>
                    )}
                </section>
            </div>

            {/* Template Editor Modal */}
            <TemplateEditorModal
                isOpen={showEditor}
                onClose={() => setShowEditor(false)}
                onCreate={createTemplate}
                columns={schema?.map(s => s.name) || []}
            />

            {/* Preview Modal */}
            {showPreview && previewData && (
                <Modal
                    isOpen={showPreview}
                    onClose={() => setShowPreview(false)}
                    title="Message Preview"
                    size="medium"
                >
                    <div className="message-preview">
                        <div className="preview-section">
                            <label>Subject:</label>
                            <p className="preview-subject">{previewData.rendered?.subject}</p>
                        </div>
                        <div className="preview-section">
                            <label>Body:</label>
                            <pre className="preview-body">{previewData.rendered?.body}</pre>
                        </div>
                        {previewData.missing_variables?.length > 0 && (
                            <div className="preview-warning">
                                ⚠️ Missing variables: {previewData.missing_variables.join(', ')}
                            </div>
                        )}
                        <div className="modal-actions">
                            <button className="btn btn-secondary" onClick={() => setShowPreview(false)}>
                                Close
                            </button>
                            <button
                                className="btn btn-primary"
                                onClick={() => sendMessages(
                                    previewData.template.id,
                                    selectedPopulation.id,
                                    false
                                )}
                            >
                                📨 Send to {selectedPopulation?.count.toLocaleString()} Recipients
                            </button>
                        </div>
                    </div>
                </Modal>
            )}
        </div>
    );
}

function TemplateEditorModal({ isOpen, onClose, onCreate, columns }) {
    const [name, setName] = useState('');
    const [subject, setSubject] = useState('');
    const [body, setBody] = useState('');
    const [channel, setChannel] = useState('email');

    const insertVariable = (varName) => {
        const insertion = `{{${varName}}}`;
        setBody(body + insertion);
    };

    const handleSubmit = (e) => {
        e.preventDefault();
        if (name && subject && body) {
            onCreate({ name, subject, body, channel });
            setName('');
            setSubject('');
            setBody('');
        }
    };

    if (!isOpen) return null;

    return (
        <Modal isOpen={isOpen} onClose={onClose} title="Create Message Template" size="large">
            <form onSubmit={handleSubmit}>
                <div className="form-row">
                    <div className="form-group flex-1">
                        <label>Template Name *</label>
                        <input
                            type="text"
                            className="prompt-input"
                            value={name}
                            onChange={(e) => setName(e.target.value)}
                            placeholder="e.g., Welcome Email"
                            required
                        />
                    </div>
                    <div className="form-group">
                        <label>Channel</label>
                        <select
                            className="chart-type-select"
                            value={channel}
                            onChange={(e) => setChannel(e.target.value)}
                        >
                            <option value="email">📧 Email</option>
                            <option value="sms">💬 SMS</option>
                        </select>
                    </div>
                </div>

                <div className="form-group">
                    <label>Subject Line *</label>
                    <input
                        type="text"
                        className="prompt-input"
                        value={subject}
                        onChange={(e) => setSubject(e.target.value)}
                        placeholder="e.g., Hello {{name}}!"
                        required
                    />
                </div>

                <div className="form-group">
                    <label>Message Body *</label>
                    <textarea
                        className="query-input"
                        value={body}
                        onChange={(e) => setBody(e.target.value)}
                        rows={8}
                        placeholder="Write your message here. Use {{column_name}} to insert data."
                        required
                    />
                </div>

                <div className="variable-picker">
                    <label>Insert Variable:</label>
                    <div className="variable-buttons">
                        {columns.map(col => (
                            <button
                                key={col}
                                type="button"
                                className="btn btn-sm"
                                onClick={() => insertVariable(col)}
                            >
                                {col}
                            </button>
                        ))}
                    </div>
                </div>

                <div className="modal-actions">
                    <button type="button" className="btn btn-secondary" onClick={onClose}>
                        Cancel
                    </button>
                    <button type="submit" className="btn btn-primary">
                        Create Template
                    </button>
                </div>
            </form>
        </Modal>
    );
}
