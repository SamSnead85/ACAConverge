import { useState, useEffect } from 'react';
import { SimpleChart, QuickStats } from './Charts';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

/**
 * Audience Insights - Analytics and visualizations for a population
 */
export default function AudienceInsights({ jobId, population, schema }) {
    const [loading, setLoading] = useState(true);
    const [stats, setStats] = useState(null);
    const [sampleData, setSampleData] = useState([]);
    const [selectedColumn, setSelectedColumn] = useState(null);
    const [columnDistribution, setColumnDistribution] = useState(null);

    const columns = schema?.map(s => s.name) || [];
    const numericColumns = schema?.filter(s =>
        ['INTEGER', 'REAL'].includes(s.sql_type)
    ).map(s => s.name) || [];

    useEffect(() => {
        if (population) {
            loadInsights();
        }
    }, [population]);

    const loadInsights = async () => {
        setLoading(true);
        try {
            // Get population stats
            const statsRes = await fetch(
                `${API_URL}/population/${population.id}/stats?job_id=${jobId}`
            );
            const statsData = await statsRes.json();
            setStats(statsData);

            // Get sample data for charts
            const dataRes = await fetch(
                `${API_URL}/population/${population.id}/data?job_id=${jobId}&limit=500`
            );
            const data = await dataRes.json();
            setSampleData(data.records || []);
        } catch (err) {
            console.error('Failed to load insights:', err);
        } finally {
            setLoading(false);
        }
    };

    const loadColumnDistribution = async (column) => {
        setSelectedColumn(column);
        try {
            // Use a query to get distribution
            const res = await fetch(`${API_URL}/query/sql`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    job_id: jobId,
                    sql: `SELECT "${column}", COUNT(*) as count FROM (${population.query}) GROUP BY "${column}" ORDER BY count DESC LIMIT 20`,
                    max_rows: 20
                })
            });
            const data = await res.json();
            setColumnDistribution({
                column,
                data: data.results || []
            });
        } catch (err) {
            console.error('Failed to load distribution:', err);
        }
    };

    if (!population) {
        return (
            <div className="insights-empty">
                <p>Select a population to view insights</p>
            </div>
        );
    }

    if (loading) {
        return (
            <div className="insights-loading">
                <div className="spinner spinner-large"></div>
                <p>Loading insights...</p>
            </div>
        );
    }

    return (
        <div className="audience-insights">
            <div className="insights-header">
                <h2>📊 Audience Insights</h2>
                <div className="insights-population">
                    <span className="pop-name">{population.name}</span>
                    <span className="pop-count">{population.count.toLocaleString()} records</span>
                </div>
            </div>

            {/* Overview Cards */}
            <div className="insights-overview">
                <div className="insight-card-large">
                    <div className="insight-icon">👥</div>
                    <div className="insight-value">{population.count.toLocaleString()}</div>
                    <div className="insight-label">Total Records</div>
                </div>
                <div className="insight-card-large">
                    <div className="insight-icon">📋</div>
                    <div className="insight-value">{columns.length}</div>
                    <div className="insight-label">Data Fields</div>
                </div>
                <div className="insight-card-large">
                    <div className="insight-icon">🔢</div>
                    <div className="insight-value">{numericColumns.length}</div>
                    <div className="insight-label">Numeric Fields</div>
                </div>
                <div className="insight-card-large">
                    <div className="insight-icon">📅</div>
                    <div className="insight-value">
                        {new Date(population.created_at).toLocaleDateString()}
                    </div>
                    <div className="insight-label">Created</div>
                </div>
            </div>

            {/* Column Statistics */}
            {stats?.columns && (
                <div className="insights-section">
                    <h3>📈 Column Statistics</h3>
                    <div className="column-stats-grid">
                        {Object.entries(stats.columns).map(([name, colStats]) => (
                            <div
                                key={name}
                                className={`column-stat-card ${selectedColumn === name ? 'selected' : ''}`}
                                onClick={() => loadColumnDistribution(name)}
                            >
                                <div className="col-stat-header">
                                    <span className="col-name">{name}</span>
                                    <span className="col-type">{colStats.type}</span>
                                </div>
                                <div className="col-stat-body">
                                    <div className="col-stat-row">
                                        <span>Unique:</span>
                                        <span>{colStats.unique_count?.toLocaleString()}</span>
                                    </div>
                                    <div className="col-stat-row">
                                        <span>Nulls:</span>
                                        <span>{colStats.null_count?.toLocaleString()}</span>
                                    </div>
                                    {colStats.min !== undefined && (
                                        <>
                                            <div className="col-stat-row">
                                                <span>Min:</span>
                                                <span>{typeof colStats.min === 'number' ? colStats.min.toLocaleString() : colStats.min}</span>
                                            </div>
                                            <div className="col-stat-row">
                                                <span>Max:</span>
                                                <span>{typeof colStats.max === 'number' ? colStats.max.toLocaleString() : colStats.max}</span>
                                            </div>
                                            <div className="col-stat-row">
                                                <span>Avg:</span>
                                                <span>{colStats.avg?.toFixed(2)}</span>
                                            </div>
                                        </>
                                    )}
                                </div>
                                {colStats.unique_count <= 20 && (
                                    <div className="col-stat-footer">
                                        Click to view distribution →
                                    </div>
                                )}
                            </div>
                        ))}
                    </div>
                </div>
            )}

            {/* Column Distribution Chart */}
            {columnDistribution && (
                <div className="insights-section">
                    <h3>📊 Distribution: {columnDistribution.column}</h3>
                    <SimpleChart
                        type="bar"
                        data={columnDistribution.data}
                        labelKey={columnDistribution.column}
                        valueKey="count"
                        title={`${columnDistribution.column} Distribution`}
                        height={300}
                    />
                </div>
            )}

            {/* Numeric Summary */}
            {numericColumns.length > 0 && sampleData.length > 0 && (
                <div className="insights-section">
                    <h3>🔢 Numeric Summary</h3>
                    <QuickStats data={sampleData} columns={numericColumns} />
                </div>
            )}

            {/* Data Quality */}
            <div className="insights-section">
                <h3>✅ Data Quality</h3>
                <div className="quality-metrics">
                    {stats?.columns && Object.entries(stats.columns).map(([name, colStats]) => {
                        const completeness = ((population.count - (colStats.null_count || 0)) / population.count * 100);
                        const uniqueness = ((colStats.unique_count || 0) / population.count * 100);

                        return (
                            <div key={name} className="quality-row">
                                <span className="quality-col">{name}</span>
                                <div className="quality-bar-container">
                                    <div
                                        className="quality-bar completeness"
                                        style={{ width: `${completeness}%` }}
                                        title={`${completeness.toFixed(1)}% complete`}
                                    />
                                </div>
                                <span className="quality-percent">{completeness.toFixed(0)}%</span>
                            </div>
                        );
                    })}
                </div>
                <div className="quality-legend">
                    <span>■ Completeness (% non-null values)</span>
                </div>
            </div>
        </div>
    );
}
