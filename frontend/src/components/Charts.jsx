import { useState, useEffect, useRef } from 'react';

// Chart types
const CHART_TYPES = {
    bar: 'Bar Chart',
    line: 'Line Chart',
    pie: 'Pie Chart',
    doughnut: 'Doughnut Chart',
    horizontalBar: 'Horizontal Bar',
    scatter: 'Scatter Plot'
};

// Color palette
const CHART_COLORS = [
    '#6366f1', '#8b5cf6', '#a855f7', '#d946ef', '#ec4899',
    '#f43f5e', '#f97316', '#eab308', '#84cc16', '#22c55e',
    '#14b8a6', '#06b6d4', '#0ea5e9', '#3b82f6', '#6366f1'
];

// Simple Chart Component (No external dependencies)
export function SimpleChart({
    type = 'bar',
    data = [],
    labelKey,
    valueKey,
    title = '',
    height = 300
}) {
    const [chartType, setChartType] = useState(type);

    // Calculate max value for scaling
    const values = data.map(d => Number(d[valueKey]) || 0);
    const maxValue = Math.max(...values, 1);
    const total = values.reduce((a, b) => a + b, 0);

    if (!data.length || !labelKey || !valueKey) {
        return (
            <div className="chart-empty">
                <p>Select columns to visualize</p>
            </div>
        );
    }

    const renderBarChart = () => (
        <div className="chart-bars">
            {data.slice(0, 20).map((item, i) => {
                const value = Number(item[valueKey]) || 0;
                const percentage = (value / maxValue) * 100;
                return (
                    <div key={i} className="chart-bar-group">
                        <div className="chart-bar-label" title={String(item[labelKey])}>
                            {String(item[labelKey]).substring(0, 15)}
                        </div>
                        <div className="chart-bar-container">
                            <div
                                className="chart-bar"
                                style={{
                                    width: `${percentage}%`,
                                    backgroundColor: CHART_COLORS[i % CHART_COLORS.length]
                                }}
                            />
                        </div>
                        <div className="chart-bar-value">{value.toLocaleString()}</div>
                    </div>
                );
            })}
        </div>
    );

    const renderHorizontalBar = () => renderBarChart();

    const renderPieChart = () => {
        let cumulativePercent = 0;
        const slices = data.slice(0, 10).map((item, i) => {
            const value = Number(item[valueKey]) || 0;
            const percent = (value / total) * 100;
            const startPercent = cumulativePercent;
            cumulativePercent += percent;
            return {
                ...item,
                value,
                percent,
                startPercent,
                color: CHART_COLORS[i % CHART_COLORS.length]
            };
        });

        return (
            <div className="chart-pie-container">
                <svg viewBox="0 0 100 100" className="chart-pie-svg">
                    {slices.map((slice, i) => {
                        const startAngle = (slice.startPercent / 100) * 360 - 90;
                        const endAngle = startAngle + (slice.percent / 100) * 360;
                        const largeArc = slice.percent > 50 ? 1 : 0;

                        const startX = 50 + 40 * Math.cos((startAngle * Math.PI) / 180);
                        const startY = 50 + 40 * Math.sin((startAngle * Math.PI) / 180);
                        const endX = 50 + 40 * Math.cos((endAngle * Math.PI) / 180);
                        const endY = 50 + 40 * Math.sin((endAngle * Math.PI) / 180);

                        const pathData = `M 50 50 L ${startX} ${startY} A 40 40 0 ${largeArc} 1 ${endX} ${endY} Z`;

                        return (
                            <path
                                key={i}
                                d={pathData}
                                fill={slice.color}
                                className="chart-pie-slice"
                            >
                                <title>{`${slice[labelKey]}: ${slice.value.toLocaleString()} (${slice.percent.toFixed(1)}%)`}</title>
                            </path>
                        );
                    })}
                </svg>
                <div className="chart-pie-legend">
                    {slices.map((slice, i) => (
                        <div key={i} className="chart-legend-item">
                            <span
                                className="chart-legend-color"
                                style={{ backgroundColor: slice.color }}
                            />
                            <span className="chart-legend-label">
                                {String(slice[labelKey]).substring(0, 20)}
                            </span>
                            <span className="chart-legend-value">
                                {slice.percent.toFixed(1)}%
                            </span>
                        </div>
                    ))}
                </div>
            </div>
        );
    };

    const renderLineChart = () => {
        const width = 400;
        const height = 200;
        const padding = 40;
        const chartWidth = width - padding * 2;
        const chartHeight = height - padding * 2;

        const points = data.slice(0, 30).map((item, i) => {
            const x = padding + (i / (data.length - 1)) * chartWidth;
            const y = height - padding - ((Number(item[valueKey]) || 0) / maxValue) * chartHeight;
            return { x, y, item };
        });

        const pathD = points.map((p, i) =>
            `${i === 0 ? 'M' : 'L'} ${p.x} ${p.y}`
        ).join(' ');

        return (
            <svg viewBox={`0 0 ${width} ${height}`} className="chart-line-svg">
                {/* Grid lines */}
                {[0, 0.25, 0.5, 0.75, 1].map((ratio, i) => (
                    <line
                        key={i}
                        x1={padding}
                        y1={height - padding - ratio * chartHeight}
                        x2={width - padding}
                        y2={height - padding - ratio * chartHeight}
                        className="chart-grid-line"
                    />
                ))}

                {/* Area fill */}
                <path
                    d={`${pathD} L ${points[points.length - 1]?.x || padding} ${height - padding} L ${padding} ${height - padding} Z`}
                    className="chart-line-area"
                />

                {/* Line */}
                <path d={pathD} className="chart-line-path" />

                {/* Points */}
                {points.map((p, i) => (
                    <circle
                        key={i}
                        cx={p.x}
                        cy={p.y}
                        r="3"
                        className="chart-line-point"
                    >
                        <title>{`${p.item[labelKey]}: ${Number(p.item[valueKey]).toLocaleString()}`}</title>
                    </circle>
                ))}
            </svg>
        );
    };

    return (
        <div className="chart-container" style={{ minHeight: height }}>
            <div className="chart-header">
                <h4 className="chart-title">{title}</h4>
                <select
                    className="chart-type-select"
                    value={chartType}
                    onChange={(e) => setChartType(e.target.value)}
                >
                    {Object.entries(CHART_TYPES).map(([value, label]) => (
                        <option key={value} value={value}>{label}</option>
                    ))}
                </select>
            </div>

            <div className="chart-content">
                {(chartType === 'bar' || chartType === 'horizontalBar') && renderBarChart()}
                {(chartType === 'pie' || chartType === 'doughnut') && renderPieChart()}
                {chartType === 'line' && renderLineChart()}
                {chartType === 'scatter' && renderLineChart()}
            </div>
        </div>
    );
}

// Quick Stats Cards
export function QuickStats({ data = [], columns = [] }) {
    const numericColumns = columns.filter(col => {
        const sample = data.find(row => row[col] !== null && row[col] !== undefined);
        return sample && typeof sample[col] === 'number';
    });

    const stats = numericColumns.slice(0, 4).map(col => {
        const values = data.map(row => Number(row[col]) || 0);
        const sum = values.reduce((a, b) => a + b, 0);
        const avg = sum / values.length;
        const min = Math.min(...values);
        const max = Math.max(...values);

        return {
            column: col,
            sum,
            avg,
            min,
            max,
            count: values.length
        };
    });

    if (stats.length === 0) {
        return null;
    }

    return (
        <div className="quick-stats">
            {stats.map((stat, i) => (
                <div key={i} className="stat-card">
                    <div className="stat-header">{stat.column}</div>
                    <div className="stat-grid">
                        <div className="stat-item">
                            <span className="stat-label">Sum</span>
                            <span className="stat-value">{stat.sum.toLocaleString()}</span>
                        </div>
                        <div className="stat-item">
                            <span className="stat-label">Avg</span>
                            <span className="stat-value">{stat.avg.toFixed(2)}</span>
                        </div>
                        <div className="stat-item">
                            <span className="stat-label">Min</span>
                            <span className="stat-value">{stat.min.toLocaleString()}</span>
                        </div>
                        <div className="stat-item">
                            <span className="stat-label">Max</span>
                            <span className="stat-value">{stat.max.toLocaleString()}</span>
                        </div>
                    </div>
                </div>
            ))}
        </div>
    );
}

// Data Insights
export function DataInsights({ data = [], columns = [] }) {
    const insights = [];

    // Total records
    insights.push({
        icon: '📊',
        title: 'Total Records',
        value: data.length.toLocaleString()
    });

    // Columns count
    insights.push({
        icon: '📋',
        title: 'Columns',
        value: columns.length
    });

    // Find nulls
    const nullCounts = columns.map(col => ({
        column: col,
        nulls: data.filter(row => row[col] === null || row[col] === undefined).length
    })).filter(c => c.nulls > 0);

    if (nullCounts.length > 0) {
        const totalNulls = nullCounts.reduce((a, b) => a + b.nulls, 0);
        insights.push({
            icon: '❓',
            title: 'Missing Values',
            value: totalNulls.toLocaleString(),
            detail: `in ${nullCounts.length} columns`
        });
    }

    // Unique value ratios for text columns
    const textColumns = columns.filter(col => {
        const sample = data.find(row => row[col] !== null);
        return sample && typeof sample[col] === 'string';
    });

    if (textColumns.length > 0 && data.length > 0) {
        const cardinality = textColumns.map(col => ({
            column: col,
            unique: new Set(data.map(row => row[col])).size
        }));
        const highCardinality = cardinality.filter(c => c.unique === data.length);
        if (highCardinality.length > 0) {
            insights.push({
                icon: '🔑',
                title: 'Potential Keys',
                value: highCardinality.length,
                detail: highCardinality.map(c => c.column).join(', ')
            });
        }
    }

    return (
        <div className="data-insights">
            {insights.map((insight, i) => (
                <div key={i} className="insight-card">
                    <span className="insight-icon">{insight.icon}</span>
                    <div className="insight-content">
                        <div className="insight-title">{insight.title}</div>
                        <div className="insight-value">{insight.value}</div>
                        {insight.detail && (
                            <div className="insight-detail">{insight.detail}</div>
                        )}
                    </div>
                </div>
            ))}
        </div>
    );
}

export default SimpleChart;
