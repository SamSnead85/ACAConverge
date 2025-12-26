import { useState, useEffect } from 'react';

export default function QueryHistory({ jobId, onSelectQuery }) {
    const [history, setHistory] = useState([]);
    const [loading, setLoading] = useState(false);

    useEffect(() => {
        if (jobId) {
            fetchHistory();
        }
    }, [jobId]);

    const fetchHistory = async () => {
        setLoading(true);
        try {
            const response = await fetch(
                `${import.meta.env.VITE_API_URL || 'http://localhost:8000/api'}/query/history/${jobId}`
            );
            const data = await response.json();
            setHistory(data.history || []);
        } catch (err) {
            console.error('Failed to fetch history:', err);
        } finally {
            setLoading(false);
        }
    };

    const formatTime = (timestamp) => {
        const date = new Date(timestamp);
        return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    };

    if (!jobId) {
        return (
            <div className="card">
                <div className="card-header">
                    <h2 className="card-title">Query History</h2>
                </div>
                <div className="empty-state">
                    <div className="empty-state-icon">📜</div>
                    <h3 className="empty-state-title">No History Yet</h3>
                    <p>Upload a file and run queries to see history</p>
                </div>
            </div>
        );
    }

    return (
        <div className="card">
            <div className="card-header">
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <div>
                        <h2 className="card-title">Query History</h2>
                        <p className="card-description">
                            {history.length} {history.length === 1 ? 'query' : 'queries'} in this session
                        </p>
                    </div>
                    <button
                        className="btn btn-secondary"
                        onClick={fetchHistory}
                        disabled={loading}
                    >
                        🔄 Refresh
                    </button>
                </div>
            </div>

            {loading ? (
                <div style={{ textAlign: 'center', padding: '2rem' }}>
                    <span className="spinner"></span>
                </div>
            ) : history.length === 0 ? (
                <div className="empty-state">
                    <div className="empty-state-icon">💬</div>
                    <h3 className="empty-state-title">No Queries Yet</h3>
                    <p>Run your first query to see it here</p>
                </div>
            ) : (
                <div className="history-list">
                    {history.map((item) => (
                        <div
                            key={item.query_id}
                            className="history-item"
                            onClick={() => onSelectQuery && onSelectQuery(item.natural_language)}
                        >
                            <div className="history-query">{item.natural_language}</div>
                            <div className="history-meta">
                                <span>
                                    {item.error ? (
                                        <span style={{ color: 'var(--color-error)' }}>❌ Error</span>
                                    ) : (
                                        `${item.row_count} rows`
                                    )}
                                </span>
                                <span>{item.execution_time_ms}ms</span>
                                <span>{formatTime(item.timestamp)}</span>
                            </div>
                            {item.sql_query && (
                                <pre style={{
                                    marginTop: '0.5rem',
                                    fontSize: '0.75rem',
                                    color: 'var(--color-text-muted)',
                                    whiteSpace: 'nowrap',
                                    overflow: 'hidden',
                                    textOverflow: 'ellipsis'
                                }}>
                                    {item.sql_query}
                                </pre>
                            )}
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
}
