import { useState, useEffect } from 'react';
import { useToast } from './Toast';
import { Spinner } from './Loading';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

/**
 * Import History - View all past file conversions and switch between them
 */
export default function ImportHistory({ currentJobId, onSelectJob }) {
    const [jobs, setJobs] = useState([]);
    const [loading, setLoading] = useState(true);
    const { addToast } = useToast();

    useEffect(() => {
        loadJobs();
    }, []);

    const loadJobs = async () => {
        try {
            const res = await fetch(`${API_URL}/jobs`);
            const data = await res.json();
            setJobs(data.jobs || []);
        } catch (err) {
            addToast('Failed to load import history', 'error');
        } finally {
            setLoading(false);
        }
    };

    const deleteJob = async (jobId) => {
        if (!confirm('Delete this import and its data?')) return;

        try {
            await fetch(`${API_URL}/jobs/${jobId}`, { method: 'DELETE' });
            setJobs(jobs.filter(j => j.job_id !== jobId));
            addToast('Import deleted', 'success');
        } catch (err) {
            addToast('Failed to delete import', 'error');
        }
    };

    const formatFileSize = (bytes) => {
        if (!bytes) return 'N/A';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
    };

    const getStatusIcon = (status) => {
        switch (status) {
            case 'completed': return '✅';
            case 'processing': return '⏳';
            case 'error': return '❌';
            default: return '⏸️';
        }
    };

    const getFileIcon = (filename) => {
        if (!filename) return '📁';
        const ext = filename.split('.').pop()?.toLowerCase();
        switch (ext) {
            case 'yxdb': return '⚡';
            case 'csv': return '📊';
            case 'xlsx':
            case 'xls': return '📗';
            case 'json': return '📋';
            default: return '📁';
        }
    };

    if (loading) {
        return (
            <div className="import-history-loading">
                <Spinner size="large" />
                <p>Loading import history...</p>
            </div>
        );
    }

    return (
        <div className="import-history">
            <div className="history-header">
                <h3>📂 Import History</h3>
                <span className="history-count">{jobs.length} imports</span>
            </div>

            {jobs.length === 0 ? (
                <div className="history-empty">
                    <p>No imports yet. Upload a file to get started.</p>
                </div>
            ) : (
                <div className="history-list">
                    {jobs.map(job => (
                        <div
                            key={job.job_id}
                            className={`history-item ${job.job_id === currentJobId ? 'active' : ''}`}
                        >
                            <div className="history-item-icon">
                                {getFileIcon(job.filename)}
                            </div>

                            <div className="history-item-info">
                                <div className="history-item-name">{job.filename}</div>
                                <div className="history-item-meta">
                                    <span>{getStatusIcon(job.status)} {job.status}</span>
                                    <span>•</span>
                                    <span>{job.file_type || 'Unknown'}</span>
                                </div>
                            </div>

                            <div className="history-item-actions">
                                {job.status === 'completed' && job.job_id !== currentJobId && (
                                    <button
                                        className="btn btn-sm btn-primary"
                                        onClick={() => onSelectJob?.(job.job_id)}
                                    >
                                        Load
                                    </button>
                                )}
                                {job.job_id === currentJobId && (
                                    <span className="current-badge">Current</span>
                                )}
                                <button
                                    className="btn btn-sm btn-danger"
                                    onClick={() => deleteJob(job.job_id)}
                                >
                                    🗑️
                                </button>
                            </div>
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
}
