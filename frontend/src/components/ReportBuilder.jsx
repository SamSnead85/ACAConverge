import { useState, useEffect } from 'react';
import { useToast } from './Toast';
import { Modal } from './Modal';
import { Spinner } from './Loading';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

export default function ReportBuilder({ jobId, populations, schema }) {
    const [selectedPops, setSelectedPops] = useState([]);
    const [reportType, setReportType] = useState('summary');
    const [selectedColumns, setSelectedColumns] = useState([]);
    const [generatedReport, setGeneratedReport] = useState(null);
    const [loading, setLoading] = useState(false);
    const [showHtml, setShowHtml] = useState(false);
    const { addToast } = useToast();

    const columns = schema?.map(s => s.name) || [];

    const togglePopulation = (popId) => {
        setSelectedPops(prev =>
            prev.includes(popId)
                ? prev.filter(id => id !== popId)
                : [...prev, popId]
        );
    };

    const toggleColumn = (col) => {
        setSelectedColumns(prev =>
            prev.includes(col)
                ? prev.filter(c => c !== col)
                : [...prev, col]
        );
    };

    const generateReport = async () => {
        if (selectedPops.length === 0) {
            addToast('Please select at least one population', 'warning');
            return;
        }

        setLoading(true);
        try {
            let url;
            let body = {};

            if (reportType === 'comparison' && selectedPops.length > 1) {
                url = `${API_URL}/report/comparison?job_id=${jobId}`;
                body = { population_ids: selectedPops };
            } else if (reportType === 'detailed') {
                url = `${API_URL}/report/detailed/${selectedPops[0]}?job_id=${jobId}`;
                body = {
                    columns: selectedColumns.length > 0 ? selectedColumns : null,
                    limit: 10000
                };
            } else {
                url = `${API_URL}/report/summary/${selectedPops[0]}?job_id=${jobId}`;
            }

            const res = await fetch(url, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body)
            });

            const data = await res.json();
            if (data.report) {
                setGeneratedReport(data.report);
                addToast('Report generated successfully', 'success');
            }
        } catch (err) {
            addToast('Failed to generate report', 'error');
        } finally {
            setLoading(false);
        }
    };

    const downloadReport = async (format) => {
        if (!selectedPops[0]) return;

        try {
            const url = `${API_URL}/report/${selectedPops[0]}/download?job_id=${jobId}&format=${format}&type=${reportType}`;
            const res = await fetch(url);

            if (res.ok) {
                const blob = await res.blob();
                const downloadUrl = window.URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = downloadUrl;
                a.download = `report.${format}`;
                a.click();
                window.URL.revokeObjectURL(downloadUrl);
                addToast(`Downloaded report as ${format.toUpperCase()}`, 'success');
            }
        } catch (err) {
            addToast('Failed to download report', 'error');
        }
    };

    const viewHtmlReport = async () => {
        if (!selectedPops[0]) return;
        setShowHtml(true);
    };

    return (
        <div className="report-builder">
            <div className="report-header">
                <h2>📊 Report Builder</h2>
            </div>

            <div className="report-config">
                {/* Population Selection */}
                <section className="config-section">
                    <h3>1. Select Population(s)</h3>
                    {populations.length === 0 ? (
                        <p className="empty-text">No populations available. Create one first.</p>
                    ) : (
                        <div className="pop-select-grid">
                            {populations.map(pop => (
                                <label key={pop.id} className="pop-select-item">
                                    <input
                                        type="checkbox"
                                        checked={selectedPops.includes(pop.id)}
                                        onChange={() => togglePopulation(pop.id)}
                                    />
                                    <span className="pop-select-name">{pop.name}</span>
                                    <span className="pop-select-count">{pop.count.toLocaleString()}</span>
                                </label>
                            ))}
                        </div>
                    )}
                </section>

                {/* Report Type */}
                <section className="config-section">
                    <h3>2. Report Type</h3>
                    <div className="report-types">
                        <label className={`report-type ${reportType === 'summary' ? 'active' : ''}`}>
                            <input
                                type="radio"
                                name="reportType"
                                value="summary"
                                checked={reportType === 'summary'}
                                onChange={(e) => setReportType(e.target.value)}
                            />
                            <div className="type-content">
                                <span className="type-icon">📈</span>
                                <span className="type-name">Summary</span>
                                <span className="type-desc">Statistics overview</span>
                            </div>
                        </label>

                        <label className={`report-type ${reportType === 'detailed' ? 'active' : ''}`}>
                            <input
                                type="radio"
                                name="reportType"
                                value="detailed"
                                checked={reportType === 'detailed'}
                                onChange={(e) => setReportType(e.target.value)}
                            />
                            <div className="type-content">
                                <span className="type-icon">📋</span>
                                <span className="type-name">Detailed</span>
                                <span className="type-desc">All records</span>
                            </div>
                        </label>

                        <label className={`report-type ${reportType === 'comparison' ? 'active' : ''}`}>
                            <input
                                type="radio"
                                name="reportType"
                                value="comparison"
                                checked={reportType === 'comparison'}
                                onChange={(e) => setReportType(e.target.value)}
                            />
                            <div className="type-content">
                                <span className="type-icon">⚖️</span>
                                <span className="type-name">Comparison</span>
                                <span className="type-desc">Compare populations</span>
                            </div>
                        </label>
                    </div>
                </section>

                {/* Column Selection (for detailed) */}
                {reportType === 'detailed' && (
                    <section className="config-section">
                        <h3>3. Select Columns (optional)</h3>
                        <div className="column-select">
                            {columns.map(col => (
                                <label key={col} className="column-item">
                                    <input
                                        type="checkbox"
                                        checked={selectedColumns.includes(col)}
                                        onChange={() => toggleColumn(col)}
                                    />
                                    {col}
                                </label>
                            ))}
                        </div>
                        <small className="form-help">
                            Leave empty to include all columns
                        </small>
                    </section>
                )}

                {/* Generate Button */}
                <div className="report-actions">
                    <button
                        className="btn btn-primary btn-lg"
                        onClick={generateReport}
                        disabled={loading || selectedPops.length === 0}
                    >
                        {loading ? <Spinner size="small" /> : '📊'} Generate Report
                    </button>
                </div>
            </div>

            {/* Generated Report */}
            {generatedReport && (
                <div className="generated-report">
                    <div className="report-result-header">
                        <h3>📄 {generatedReport.name}</h3>
                        <div className="report-downloads">
                            <button className="btn btn-sm" onClick={viewHtmlReport}>
                                👁️ View
                            </button>
                            <button className="btn btn-sm" onClick={() => downloadReport('csv')}>
                                📥 CSV
                            </button>
                            <button className="btn btn-sm" onClick={() => downloadReport('json')}>
                                📥 JSON
                            </button>
                        </div>
                    </div>

                    {/* Summary Stats */}
                    {reportType === 'summary' && generatedReport.data?.statistics && (
                        <div className="report-summary">
                            <div className="summary-stat">
                                <span className="stat-label">Total Records</span>
                                <span className="stat-value">
                                    {generatedReport.data.statistics.total_records?.toLocaleString()}
                                </span>
                            </div>

                            <h4>Column Statistics</h4>
                            <table className="data-table">
                                <thead>
                                    <tr>
                                        <th>Column</th>
                                        <th>Type</th>
                                        <th>Unique</th>
                                        <th>Nulls</th>
                                        <th>Min</th>
                                        <th>Max</th>
                                        <th>Avg</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {Object.entries(generatedReport.data.statistics.columns || {}).map(([name, stats]) => (
                                        <tr key={name}>
                                            <td>{name}</td>
                                            <td>{stats.type}</td>
                                            <td>{stats.unique_count}</td>
                                            <td>{stats.null_count}</td>
                                            <td>{stats.min ?? '-'}</td>
                                            <td>{stats.max ?? '-'}</td>
                                            <td>{stats.avg ? stats.avg.toFixed(2) : '-'}</td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    )}

                    {/* Comparison */}
                    {reportType === 'comparison' && generatedReport.data?.populations && (
                        <div className="report-comparison">
                            <div className="comparison-summary">
                                <div className="summary-stat">
                                    <span className="stat-label">Populations Compared</span>
                                    <span className="stat-value">
                                        {generatedReport.data.summary.total_populations}
                                    </span>
                                </div>
                                <div className="summary-stat">
                                    <span className="stat-label">Combined Records</span>
                                    <span className="stat-value">
                                        {generatedReport.data.summary.combined_records?.toLocaleString()}
                                    </span>
                                </div>
                            </div>

                            <table className="data-table">
                                <thead>
                                    <tr>
                                        <th>Population</th>
                                        <th>Records</th>
                                        <th>Created</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {generatedReport.data.populations.map((p, i) => (
                                        <tr key={i}>
                                            <td>{p.population.name}</td>
                                            <td>{p.population.count.toLocaleString()}</td>
                                            <td>{new Date(p.population.created_at).toLocaleDateString()}</td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    )}

                    {/* Detailed Preview */}
                    {reportType === 'detailed' && generatedReport.data?.records && (
                        <div className="report-detailed">
                            <p>Showing {generatedReport.data.records.length} of {generatedReport.data.total?.toLocaleString()} records</p>
                            <p className="form-help">Download the full report for all data.</p>
                        </div>
                    )}
                </div>
            )}

            {/* HTML Report Modal */}
            {showHtml && (
                <Modal
                    isOpen={showHtml}
                    onClose={() => setShowHtml(false)}
                    title="Report Preview"
                    size="fullscreen"
                >
                    <iframe
                        src={`${API_URL}/report/${selectedPops[0]}/html?job_id=${jobId}&type=${reportType}`}
                        style={{ width: '100%', height: '80vh', border: 'none' }}
                        title="Report Preview"
                    />
                </Modal>
            )}
        </div>
    );
}
