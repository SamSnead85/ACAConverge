import { useState } from 'react';
import { exportToCSV, exportToJSON } from '../utils/api';
import { useToast } from './Toast';
import { Modal } from './Modal';
import QueryBuilder from './QueryBuilder';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

export default function QueryInterface({ jobId, schema, onSavePopulation }) {
    const [question, setQuestion] = useState('');
    const [sqlPreview, setSqlPreview] = useState('');
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [result, setResult] = useState(null);
    const [showSql, setShowSql] = useState(false);
    const [showBuilder, setShowBuilder] = useState(false);
    const [showSavePopulation, setShowSavePopulation] = useState(false);
    const [populationName, setPopulationName] = useState('');
    const [saving, setSaving] = useState(false);
    const { addToast } = useToast();

    const handleSubmit = async (e) => {
        e.preventDefault();
        if (!question.trim() || !jobId) return;

        setLoading(true);
        setError(null);
        setSqlPreview('');

        try {
            const response = await fetch(`${API_URL}/query`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    job_id: jobId,
                    question: question.trim(),
                    max_rows: 1000,
                }),
            });

            const data = await response.json();

            if (data.error) {
                setError(data.error);
            } else {
                setResult(data);
                setSqlPreview(data.sql_query);
            }
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    const handleSqlQuery = async (sql) => {
        setLoading(true);
        setError(null);
        setSqlPreview(sql);
        setShowBuilder(false);

        try {
            const response = await fetch(`${API_URL}/query/sql`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    job_id: jobId,
                    sql: sql,
                    max_rows: 1000,
                }),
            });

            const data = await response.json();

            if (data.error) {
                setError(data.error);
            } else {
                setResult(data);
            }
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    const handleQueryBuilderResult = (sql) => {
        setQuestion('');
        handleSqlQuery(sql);
    };

    const handleSaveAsPopulation = async () => {
        if (!populationName.trim() || !result) return;

        setSaving(true);
        try {
            const response = await fetch(`${API_URL}/populations/${jobId}`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    name: populationName.trim(),
                    query: sqlPreview,
                    description: question || `Created from query: ${sqlPreview.substring(0, 50)}...`
                }),
            });

            const data = await response.json();

            if (data.population) {
                addToast(`Population "${populationName}" saved with ${data.population.count} records`, 'success');
                setShowSavePopulation(false);
                setPopulationName('');
                onSavePopulation?.();
            } else if (data.detail) {
                addToast(data.detail, 'error');
            }
        } catch (err) {
            addToast('Failed to save population', 'error');
        } finally {
            setSaving(false);
        }
    };

    const handleExportCSV = () => {
        if (result && result.results.length > 0) {
            exportToCSV(result.results, result.columns, `query_${result.query_id}.csv`);
        }
    };

    const handleExportJSON = () => {
        if (result && result.results.length > 0) {
            exportToJSON(result.results, `query_${result.query_id}.json`);
        }
    };

    const sampleQueries = [
        'Show all records',
        'Count total records',
        'Show me records where sales > 1000',
        'Get the top 10 records by sales',
        'Show unique regions',
        'Average sales by region',
    ];

    if (!jobId) {
        return (
            <div className="card">
                <div className="empty-state">
                    <div className="empty-state-icon">💬</div>
                    <h3 className="empty-state-title">No Database Selected</h3>
                    <p>Upload and convert a file first to start querying</p>
                </div>
            </div>
        );
    }

    return (
        <div className="query-page">
            {/* Query Builder Toggle */}
            <div className="query-mode-toggle">
                <button
                    className={`mode-btn ${!showBuilder ? 'active' : ''}`}
                    onClick={() => setShowBuilder(false)}
                >
                    💬 Natural Language
                </button>
                <button
                    className={`mode-btn ${showBuilder ? 'active' : ''}`}
                    onClick={() => setShowBuilder(true)}
                >
                    🔧 Query Builder
                </button>
            </div>

            {showBuilder ? (
                <QueryBuilder
                    schema={schema}
                    onQueryBuilt={handleQueryBuilderResult}
                />
            ) : (
                <div className="card">
                    <div className="card-header">
                        <h2 className="card-title">Natural Language Query</h2>
                        <p className="card-description">
                            Ask questions about your data in plain English - AI will convert to SQL
                        </p>
                    </div>

                    {error && (
                        <div className="alert alert-error">
                            {error}
                        </div>
                    )}

                    <form onSubmit={handleSubmit}>
                        <div className="query-input-group">
                            <textarea
                                className="query-input"
                                value={question}
                                onChange={(e) => setQuestion(e.target.value)}
                                placeholder="Ask a question about your data... e.g., 'Show me all records where sales > 1000'"
                                rows={3}
                            />
                        </div>

                        <div className="sample-queries">
                            {sampleQueries.map((q) => (
                                <button
                                    key={q}
                                    type="button"
                                    className="sample-query-btn"
                                    onClick={() => setQuestion(q)}
                                >
                                    {q}
                                </button>
                            ))}
                        </div>

                        <div className="query-actions">
                            <button
                                type="submit"
                                className="btn btn-primary"
                                disabled={loading || !question.trim()}
                            >
                                {loading ? (
                                    <>
                                        <span className="spinner"></span>
                                        Processing...
                                    </>
                                ) : (
                                    '🔍 Run Query'
                                )}
                            </button>

                            {result && (
                                <button
                                    type="button"
                                    className="btn btn-secondary"
                                    onClick={() => setShowSql(!showSql)}
                                >
                                    {showSql ? 'Hide SQL' : 'Show SQL'}
                                </button>
                            )}
                        </div>
                    </form>
                </div>
            )}

            {showSql && sqlPreview && (
                <div className="sql-preview">
                    <div className="sql-preview-header">
                        <span className="sql-preview-title">Generated SQL</span>
                    </div>
                    <pre className="sql-code">{sqlPreview}</pre>
                </div>
            )}

            {result && result.results.length > 0 && (
                <div className="results-container">
                    <div className="results-header">
                        <span className="results-count">
                            {result.row_count} result{result.row_count !== 1 ? 's' : ''}
                            {' • '}
                            {result.execution_time_ms}ms
                        </span>
                        <div className="results-actions">
                            <button
                                className="btn btn-accent"
                                onClick={() => setShowSavePopulation(true)}
                            >
                                💾 Save as Population
                            </button>
                            <button className="btn btn-secondary" onClick={handleExportCSV}>
                                📥 CSV
                            </button>
                            <button className="btn btn-secondary" onClick={handleExportJSON}>
                                📥 JSON
                            </button>
                        </div>
                    </div>

                    <div className="results-table-wrapper" style={{ maxHeight: '400px', overflowY: 'auto' }}>
                        <table className="results-table">
                            <thead>
                                <tr>
                                    {result.columns.map((col) => (
                                        <th key={col}>{col}</th>
                                    ))}
                                </tr>
                            </thead>
                            <tbody>
                                {result.results.slice(0, 100).map((row, idx) => (
                                    <tr key={idx}>
                                        {result.columns.map((col) => (
                                            <td key={col} title={String(row[col] ?? '')}>
                                                {row[col] === null ? (
                                                    <span style={{ color: 'var(--color-text-muted)' }}>null</span>
                                                ) : (
                                                    String(row[col])
                                                )}
                                            </td>
                                        ))}
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>

                    {result.results.length > 100 && (
                        <p className="results-more">
                            Showing first 100 of {result.row_count} results. Export or save as population for complete data.
                        </p>
                    )}
                </div>
            )}

            {result && result.results.length === 0 && !error && (
                <div className="alert alert-info" style={{ marginTop: '1rem' }}>
                    Query executed successfully but returned no results.
                </div>
            )}

            {/* Save as Population Modal */}
            <Modal
                isOpen={showSavePopulation}
                onClose={() => setShowSavePopulation(false)}
                title="Save as Population"
                size="small"
            >
                <div className="save-population-form">
                    <p>Save these {result?.row_count.toLocaleString()} records as a reusable population for reports and messaging.</p>

                    <div className="form-group">
                        <label>Population Name</label>
                        <input
                            type="text"
                            className="prompt-input"
                            value={populationName}
                            onChange={(e) => setPopulationName(e.target.value)}
                            placeholder="e.g., High Value Customers"
                        />
                    </div>

                    <div className="form-group">
                        <label>Source Query</label>
                        <pre className="sql-preview-small">{sqlPreview}</pre>
                    </div>

                    <div className="modal-actions">
                        <button
                            className="btn btn-secondary"
                            onClick={() => setShowSavePopulation(false)}
                        >
                            Cancel
                        </button>
                        <button
                            className="btn btn-primary"
                            onClick={handleSaveAsPopulation}
                            disabled={saving || !populationName.trim()}
                        >
                            {saving ? 'Saving...' : '💾 Save Population'}
                        </button>
                    </div>
                </div>
            </Modal>
        </div>
    );
}
