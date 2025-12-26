// API utility functions for YXDB Converter

const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

/**
 * Upload a file for conversion
 */
export async function uploadFile(file, useMock = false) {
    const formData = new FormData();
    formData.append('file', file);

    const response = await fetch(`${API_BASE}/upload?use_mock=${useMock}`, {
        method: 'POST',
        body: formData,
    });

    if (!response.ok) {
        throw new Error('Upload failed');
    }

    return response.json();
}

/**
 * Start a demo conversion with mock data
 */
export async function startDemo() {
    const response = await fetch(`${API_BASE}/upload/demo`, {
        method: 'POST',
    });

    if (!response.ok) {
        throw new Error('Demo failed to start');
    }

    return response.json();
}

/**
 * Get conversion status
 */
export async function getConversionStatus(jobId) {
    const response = await fetch(`${API_BASE}/conversion/status/${jobId}`);

    if (!response.ok) {
        throw new Error('Failed to get status');
    }

    return response.json();
}

/**
 * Get schema for a converted database
 */
export async function getSchema(jobId) {
    const response = await fetch(`${API_BASE}/schema/${jobId}`);

    if (!response.ok) {
        throw new Error('Failed to get schema');
    }

    return response.json();
}

/**
 * Submit a natural language query
 */
export async function submitQuery(jobId, question, maxRows = 1000) {
    const response = await fetch(`${API_BASE}/query`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            job_id: jobId,
            question,
            max_rows: maxRows,
        }),
    });

    if (!response.ok) {
        throw new Error('Query failed');
    }

    return response.json();
}

/**
 * Submit a direct SQL query
 */
export async function submitSqlQuery(jobId, sql, maxRows = 1000) {
    const response = await fetch(`${API_BASE}/query/sql`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            job_id: jobId,
            sql,
            max_rows: maxRows,
        }),
    });

    if (!response.ok) {
        throw new Error('SQL query failed');
    }

    return response.json();
}

/**
 * Get query history
 */
export async function getQueryHistory(jobId, limit = 20) {
    const response = await fetch(`${API_BASE}/query/history/${jobId}?limit=${limit}`);

    if (!response.ok) {
        throw new Error('Failed to get history');
    }

    return response.json();
}

/**
 * Download database file
 */
export function getDownloadUrl(jobId) {
    return `${API_BASE}/download/${jobId}`;
}

/**
 * Export query results
 */
export function getExportUrl(jobId, queryId, format = 'json') {
    return `${API_BASE}/query/export/${jobId}/${queryId}?format=${format}`;
}

/**
 * Export data to CSV
 */
export function exportToCSV(data, columns, filename = 'export.csv') {
    const header = columns.join(',');
    const rows = data.map(row =>
        columns.map(col => {
            const val = row[col];
            if (val === null || val === undefined) return '';
            const str = String(val);
            if (str.includes(',') || str.includes('"') || str.includes('\n')) {
                return `"${str.replace(/"/g, '""')}"`;
            }
            return str;
        }).join(',')
    );

    const csv = [header, ...rows].join('\n');
    downloadBlob(csv, filename, 'text/csv');
}

/**
 * Export data to JSON
 */
export function exportToJSON(data, filename = 'export.json') {
    const json = JSON.stringify(data, null, 2);
    downloadBlob(json, filename, 'application/json');
}

/**
 * Download a blob as a file
 */
function downloadBlob(content, filename, type) {
    const blob = new Blob([content], { type });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
}
