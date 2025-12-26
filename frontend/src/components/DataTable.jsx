import { useState, useMemo } from 'react';
import { exportToCSV, exportToJSON } from '../utils/api';

export default function DataTable({
    data = [],
    columns = [],
    pageSize = 25,
    sortable = true,
    filterable = true,
    exportable = true,
    onRowClick = null,
    emptyMessage = 'No data available'
}) {
    const [currentPage, setCurrentPage] = useState(1);
    const [sortConfig, setSortConfig] = useState({ key: null, direction: 'asc' });
    const [filters, setFilters] = useState({});
    const [columnWidths, setColumnWidths] = useState({});

    // Filter data
    const filteredData = useMemo(() => {
        if (!filterable || Object.keys(filters).length === 0) return data;

        return data.filter(row => {
            return Object.entries(filters).every(([key, value]) => {
                if (!value) return true;
                const cellValue = String(row[key] ?? '').toLowerCase();
                return cellValue.includes(value.toLowerCase());
            });
        });
    }, [data, filters, filterable]);

    // Sort data
    const sortedData = useMemo(() => {
        if (!sortable || !sortConfig.key) return filteredData;

        return [...filteredData].sort((a, b) => {
            const aVal = a[sortConfig.key];
            const bVal = b[sortConfig.key];

            if (aVal === null || aVal === undefined) return 1;
            if (bVal === null || bVal === undefined) return -1;

            if (typeof aVal === 'number' && typeof bVal === 'number') {
                return sortConfig.direction === 'asc' ? aVal - bVal : bVal - aVal;
            }

            const aStr = String(aVal);
            const bStr = String(bVal);
            return sortConfig.direction === 'asc'
                ? aStr.localeCompare(bStr)
                : bStr.localeCompare(aStr);
        });
    }, [filteredData, sortConfig, sortable]);

    // Paginate data
    const paginatedData = useMemo(() => {
        const start = (currentPage - 1) * pageSize;
        return sortedData.slice(start, start + pageSize);
    }, [sortedData, currentPage, pageSize]);

    const totalPages = Math.ceil(sortedData.length / pageSize);

    const handleSort = (key) => {
        if (!sortable) return;
        setSortConfig(prev => ({
            key,
            direction: prev.key === key && prev.direction === 'asc' ? 'desc' : 'asc'
        }));
    };

    const handleFilter = (key, value) => {
        setFilters(prev => ({ ...prev, [key]: value }));
        setCurrentPage(1);
    };

    const handleExportCSV = () => {
        exportToCSV(sortedData, columns, 'export.csv');
    };

    const handleExportJSON = () => {
        exportToJSON(sortedData, 'export.json');
    };

    const getSortIcon = (key) => {
        if (sortConfig.key !== key) return '⇅';
        return sortConfig.direction === 'asc' ? '↑' : '↓';
    };

    if (data.length === 0) {
        return (
            <div className="empty-state">
                <div className="empty-state-icon">📊</div>
                <h3 className="empty-state-title">{emptyMessage}</h3>
            </div>
        );
    }

    return (
        <div className="data-table-container">
            {/* Toolbar */}
            <div className="data-table-toolbar">
                <div className="data-table-info">
                    Showing {paginatedData.length} of {sortedData.length} records
                    {Object.keys(filters).some(k => filters[k]) && (
                        <button
                            className="btn-link"
                            onClick={() => setFilters({})}
                        >
                            Clear filters
                        </button>
                    )}
                </div>
                {exportable && (
                    <div className="data-table-actions">
                        <button className="btn btn-secondary btn-sm" onClick={handleExportCSV}>
                            📥 CSV
                        </button>
                        <button className="btn btn-secondary btn-sm" onClick={handleExportJSON}>
                            📥 JSON
                        </button>
                    </div>
                )}
            </div>

            {/* Table */}
            <div className="results-table-wrapper">
                <table className="results-table data-table">
                    <thead>
                        {/* Header Row */}
                        <tr>
                            {columns.map(col => (
                                <th
                                    key={col}
                                    onClick={() => handleSort(col)}
                                    className={sortable ? 'sortable' : ''}
                                    style={columnWidths[col] ? { width: columnWidths[col] } : {}}
                                >
                                    <div className="th-content">
                                        <span>{col}</span>
                                        {sortable && <span className="sort-icon">{getSortIcon(col)}</span>}
                                    </div>
                                </th>
                            ))}
                        </tr>
                        {/* Filter Row */}
                        {filterable && (
                            <tr className="filter-row">
                                {columns.map(col => (
                                    <th key={`filter-${col}`}>
                                        <input
                                            type="text"
                                            placeholder="Filter..."
                                            value={filters[col] || ''}
                                            onChange={(e) => handleFilter(col, e.target.value)}
                                            className="filter-input"
                                        />
                                    </th>
                                ))}
                            </tr>
                        )}
                    </thead>
                    <tbody>
                        {paginatedData.map((row, idx) => (
                            <tr
                                key={idx}
                                onClick={() => onRowClick && onRowClick(row)}
                                className={onRowClick ? 'clickable' : ''}
                            >
                                {columns.map(col => (
                                    <td key={col} title={String(row[col] ?? '')}>
                                        {row[col] === null ? (
                                            <span className="null-value">null</span>
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

            {/* Pagination */}
            {totalPages > 1 && (
                <div className="pagination">
                    <button
                        className="pagination-btn"
                        onClick={() => setCurrentPage(1)}
                        disabled={currentPage === 1}
                    >
                        ««
                    </button>
                    <button
                        className="pagination-btn"
                        onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
                        disabled={currentPage === 1}
                    >
                        «
                    </button>

                    <div className="pagination-info">
                        Page {currentPage} of {totalPages}
                    </div>

                    <button
                        className="pagination-btn"
                        onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))}
                        disabled={currentPage === totalPages}
                    >
                        »
                    </button>
                    <button
                        className="pagination-btn"
                        onClick={() => setCurrentPage(totalPages)}
                        disabled={currentPage === totalPages}
                    >
                        »»
                    </button>
                </div>
            )}
        </div>
    );
}
