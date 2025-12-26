export default function SchemaViewer({ schema, tableName, jobId }) {
    if (!schema || schema.length === 0) {
        return (
            <div className="card">
                <div className="empty-state">
                    <div className="empty-state-icon">📋</div>
                    <h3 className="empty-state-title">No Schema Available</h3>
                    <p>Upload a file first to view its schema</p>
                </div>
            </div>
        );
    }

    const handleDownload = () => {
        const url = `${import.meta.env.VITE_API_URL || 'http://localhost:8000/api'}/download/${jobId}`;
        window.open(url, '_blank');
    };

    return (
        <div className="card">
            <div className="card-header">
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                    <div>
                        <h2 className="card-title">Database Schema</h2>
                        <p className="card-description">
                            Table: <code style={{ color: 'var(--color-accent-secondary)' }}>{tableName || 'converted_data'}</code>
                            {' • '}
                            {schema.length} columns
                        </p>
                    </div>
                    <button className="btn btn-success" onClick={handleDownload}>
                        ⬇️ Download SQLite
                    </button>
                </div>
            </div>

            <div className="results-table-wrapper">
                <table className="schema-table">
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>Column Name</th>
                            <th>YXDB Type</th>
                            <th>SQL Type</th>
                            <th>Size</th>
                        </tr>
                    </thead>
                    <tbody>
                        {schema.map((field, index) => (
                            <tr key={field.name}>
                                <td style={{ color: 'var(--color-text-muted)' }}>{index + 1}</td>
                                <td>
                                    <strong>{field.name}</strong>
                                </td>
                                <td>
                                    <span className="type-badge">{field.original_type}</span>
                                </td>
                                <td>{field.sql_type}</td>
                                <td style={{ color: 'var(--color-text-muted)' }}>
                                    {field.size > 0 ? field.size : '—'}
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
        </div>
    );
}
