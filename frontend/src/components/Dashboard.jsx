import { useState, useEffect } from 'react';
import { SimpleChart, QuickStats, DataInsights } from './Charts';
import DataTable from './DataTable';

export default function Dashboard({ jobId, schema }) {
    const [loading, setLoading] = useState(true);
    const [stats, setStats] = useState(null);
    const [sampleData, setSampleData] = useState([]);
    const [error, setError] = useState(null);

    useEffect(() => {
        if (jobId) {
            loadDashboardData();
        }
    }, [jobId]);

    const loadDashboardData = async () => {
        setLoading(true);
        setError(null);

        try {
            // Get sample data and stats
            const response = await fetch(
                `${import.meta.env.VITE_API_URL || 'http://localhost:8000/api'}/query`,
                {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        job_id: jobId,
                        question: 'Show first 100 records',
                        max_rows: 100
                    })
                }
            );

            const data = await response.json();

            if (data.error) {
                setError(data.error);
            } else {
                setSampleData(data.results);

                // Get aggregation stats
                const statsResponse = await fetch(
                    `${import.meta.env.VITE_API_URL || 'http://localhost:8000/api'}/query`,
                    {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            job_id: jobId,
                            question: 'Count total records',
                            max_rows: 1
                        })
                    }
                );

                const statsData = await statsResponse.json();
                if (!statsData.error && statsData.results.length > 0) {
                    setStats({
                        totalRecords: Object.values(statsData.results[0])[0] || 0
                    });
                }
            }
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    if (!jobId) {
        return (
            <div className="card">
                <div className="empty-state">
                    <div className="empty-state-icon">📊</div>
                    <h3 className="empty-state-title">No Data Available</h3>
                    <p>Upload and convert a file to see the dashboard</p>
                </div>
            </div>
        );
    }

    if (loading) {
        return (
            <div className="dashboard-loading">
                <div className="spinner spinner-large"></div>
                <p>Loading dashboard...</p>
            </div>
        );
    }

    const columns = schema?.map(s => s.name) || [];
    const numericColumns = schema?.filter(s =>
        ['INTEGER', 'REAL'].includes(s.sql_type)
    ).map(s => s.name) || [];

    return (
        <div className="dashboard">
            {/* Header Stats */}
            <div className="dashboard-header">
                <div className="dashboard-stat-cards">
                    <div className="dashboard-stat">
                        <span className="dashboard-stat-icon">📊</span>
                        <div>
                            <div className="dashboard-stat-value">
                                {stats?.totalRecords?.toLocaleString() || 'N/A'}
                            </div>
                            <div className="dashboard-stat-label">Total Records</div>
                        </div>
                    </div>
                    <div className="dashboard-stat">
                        <span className="dashboard-stat-icon">📋</span>
                        <div>
                            <div className="dashboard-stat-value">{columns.length}</div>
                            <div className="dashboard-stat-label">Columns</div>
                        </div>
                    </div>
                    <div className="dashboard-stat">
                        <span className="dashboard-stat-icon">🔢</span>
                        <div>
                            <div className="dashboard-stat-value">{numericColumns.length}</div>
                            <div className="dashboard-stat-label">Numeric Fields</div>
                        </div>
                    </div>
                </div>
            </div>

            {error && (
                <div className="alert alert-error">{error}</div>
            )}

            {/* Quick Insights */}
            {sampleData.length > 0 && (
                <div className="dashboard-section">
                    <h3 className="dashboard-section-title">📈 Quick Insights</h3>
                    <DataInsights data={sampleData} columns={columns} />
                </div>
            )}

            {/* Numeric Stats */}
            {numericColumns.length > 0 && sampleData.length > 0 && (
                <div className="dashboard-section">
                    <h3 className="dashboard-section-title">📊 Numeric Summary</h3>
                    <QuickStats data={sampleData} columns={numericColumns} />
                </div>
            )}

            {/* Charts */}
            {numericColumns.length > 0 && sampleData.length > 0 && (
                <div className="dashboard-section">
                    <h3 className="dashboard-section-title">📉 Visualization</h3>
                    <div className="dashboard-charts">
                        <SimpleChart
                            type="bar"
                            data={sampleData.slice(0, 10)}
                            labelKey={columns[0]}
                            valueKey={numericColumns[0]}
                            title={`${numericColumns[0]} by ${columns[0]}`}
                        />
                    </div>
                </div>
            )}

            {/* Sample Data Preview */}
            <div className="dashboard-section">
                <h3 className="dashboard-section-title">📋 Data Preview</h3>
                <DataTable
                    data={sampleData.slice(0, 10)}
                    columns={columns}
                    pageSize={10}
                    sortable={false}
                    filterable={false}
                    exportable={false}
                />
            </div>
        </div>
    );
}
