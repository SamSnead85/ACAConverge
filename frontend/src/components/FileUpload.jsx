import { useState, useRef, useCallback, useEffect } from 'react';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

// Supported file formats
const SUPPORTED_FORMATS = {
    '.yxdb': 'Alteryx Database',
    '.csv': 'CSV File',
    '.xlsx': 'Excel Workbook',
    '.xls': 'Excel 97-2003',
    '.json': 'JSON File'
};

const ACCEPT_TYPES = Object.keys(SUPPORTED_FORMATS).join(',');

export default function FileUpload({ onUploadComplete }) {
    const [isDragging, setIsDragging] = useState(false);
    const [file, setFile] = useState(null);
    const [uploading, setUploading] = useState(false);
    const [uploadProgress, setUploadProgress] = useState(0);
    const [progress, setProgress] = useState(null);
    const [error, setError] = useState(null);
    const fileInputRef = useRef(null);

    const handleDragOver = useCallback((e) => {
        e.preventDefault();
        setIsDragging(true);
    }, []);

    const handleDragLeave = useCallback((e) => {
        e.preventDefault();
        setIsDragging(false);
    }, []);

    const handleDrop = useCallback((e) => {
        e.preventDefault();
        setIsDragging(false);

        const droppedFile = e.dataTransfer.files[0];
        if (droppedFile) {
            validateAndSetFile(droppedFile);
        }
    }, []);

    const handleFileSelect = useCallback((e) => {
        const selectedFile = e.target.files[0];
        if (selectedFile) {
            validateAndSetFile(selectedFile);
        }
    }, []);

    const validateAndSetFile = (selectedFile) => {
        const ext = '.' + selectedFile.name.split('.').pop().toLowerCase();

        if (!SUPPORTED_FORMATS[ext]) {
            setError(`Unsupported file format. Supported: ${Object.keys(SUPPORTED_FORMATS).join(', ')}`);
            return;
        }

        setFile(selectedFile);
        setError(null);
    };

    const handleUpload = async () => {
        if (!file) return;

        setUploading(true);
        setError(null);
        setUploadProgress(0);
        setProgress({ status: 'uploading', percentage: 0, message: 'Uploading file...' });

        try {
            const formData = new FormData();
            formData.append('file', file);

            // Use XMLHttpRequest for upload progress tracking
            const xhr = new XMLHttpRequest();

            const uploadPromise = new Promise((resolve, reject) => {
                xhr.upload.onprogress = (e) => {
                    if (e.lengthComputable) {
                        const percent = Math.round((e.loaded / e.total) * 100);
                        setUploadProgress(percent);
                        setProgress({
                            status: 'uploading',
                            percentage: percent * 0.3, // Upload is 30% of total
                            message: `Uploading: ${formatFileSize(e.loaded)} / ${formatFileSize(e.total)}`
                        });
                    }
                };

                xhr.onload = () => {
                    if (xhr.status >= 200 && xhr.status < 300) {
                        try {
                            resolve(JSON.parse(xhr.responseText));
                        } catch {
                            reject(new Error('Invalid response'));
                        }
                    } else {
                        try {
                            const err = JSON.parse(xhr.responseText);
                            reject(new Error(err.detail || 'Upload failed'));
                        } catch {
                            reject(new Error(`Upload failed: ${xhr.statusText}`));
                        }
                    }
                };

                xhr.onerror = () => reject(new Error('Network error - please check your connection'));
                xhr.ontimeout = () => reject(new Error('Upload timed out'));

                xhr.open('POST', `${API_URL}/upload`);
                xhr.timeout = 0; // No timeout for large files
                xhr.send(formData);
            });

            const data = await uploadPromise;
            setProgress({
                status: 'processing',
                percentage: 30,
                message: `Processing ${data.file_type || 'file'}...`
            });

            // Poll for progress
            pollProgress(data.job_id);
        } catch (err) {
            console.error('Upload error:', err);
            setError(err.message || 'Failed to upload file');
            setUploading(false);
            setProgress(null);
        }
    };

    const pollProgress = async (jobId) => {
        try {
            const response = await fetch(`${API_URL}/conversion/status/${jobId}`);

            if (!response.ok) {
                throw new Error('Failed to check status');
            }

            const data = await response.json();

            // Map conversion progress (30-100%)
            const conversionPercent = (data.progress.percentage || 0) * 0.7 + 30;
            setProgress({
                ...data.progress,
                percentage: Math.min(conversionPercent, 100)
            });

            if (data.progress.status === 'completed') {
                setUploading(false);
                onUploadComplete(jobId, data.schema);
            } else if (data.progress.status === 'error') {
                setError(data.progress.message || 'Conversion failed');
                setUploading(false);
            } else {
                // Continue polling
                setTimeout(() => pollProgress(jobId), 500);
            }
        } catch (err) {
            setError('Failed to check progress');
            setUploading(false);
        }
    };

    const handleDemo = async () => {
        setUploading(true);
        setError(null);
        setProgress({ status: 'processing', percentage: 0, message: 'Generating demo data...' });

        try {
            const response = await fetch(`${API_URL}/upload/demo`, { method: 'POST' });

            if (!response.ok) {
                throw new Error('Demo failed to start');
            }

            const data = await response.json();
            pollProgress(data.job_id);
        } catch (err) {
            setError(err.message);
            setUploading(false);
            setProgress(null);
        }
    };

    const formatFileSize = (bytes) => {
        if (bytes === 0) return '0 Bytes';
        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    };

    const getFileIcon = (filename) => {
        if (!filename) return '📁';
        const ext = '.' + filename.split('.').pop().toLowerCase();
        switch (ext) {
            case '.yxdb': return '⚡';
            case '.csv': return '📊';
            case '.xlsx':
            case '.xls': return '📗';
            case '.json': return '📋';
            default: return '📁';
        }
    };

    const resetUpload = () => {
        setFile(null);
        setProgress(null);
        setError(null);
        setUploading(false);
        setUploadProgress(0);
    };

    return (
        <div className="card">
            <div className="card-header">
                <h2 className="card-title">Upload Data File</h2>
                <p className="card-description">
                    Upload your data file to convert it to a SQL database for querying and analysis
                </p>
            </div>

            {error && (
                <div className="alert alert-error">
                    <span>⚠️ {error}</span>
                    <button className="btn btn-sm" onClick={resetUpload}>Try Again</button>
                </div>
            )}

            {!progress && (
                <>
                    <div
                        className={`upload-zone ${isDragging ? 'dragover' : ''} ${file ? 'has-file' : ''}`}
                        onDragOver={handleDragOver}
                        onDragLeave={handleDragLeave}
                        onDrop={handleDrop}
                        onClick={() => fileInputRef.current?.click()}
                    >
                        <div className="upload-icon">{getFileIcon(file?.name)}</div>
                        <h3 className="upload-title">
                            {file ? file.name : 'Drop your data file here'}
                        </h3>
                        <p className="upload-subtitle">
                            {file
                                ? `${formatFileSize(file.size)} • ${SUPPORTED_FORMATS['.' + file.name.split('.').pop().toLowerCase()] || 'Unknown'}`
                                : 'or click to browse'}
                        </p>
                        <div className="upload-hint">
                            <strong>Supported formats:</strong><br />
                            {Object.entries(SUPPORTED_FORMATS).map(([ext, name]) => (
                                <span key={ext} className="format-badge">{ext}</span>
                            ))}
                            <br /><br />
                            <span className="size-hint">Max size: 50 GB • Large files supported</span>
                        </div>
                        <input
                            type="file"
                            ref={fileInputRef}
                            onChange={handleFileSelect}
                            accept={ACCEPT_TYPES}
                            style={{ display: 'none' }}
                        />
                    </div>

                    <div className="upload-actions">
                        <button
                            className="btn btn-primary btn-lg"
                            onClick={handleUpload}
                            disabled={!file || uploading}
                        >
                            {uploading ? 'Converting...' : '⚡ Convert to SQL'}
                        </button>
                        <button
                            className="btn btn-secondary"
                            onClick={handleDemo}
                            disabled={uploading}
                        >
                            🎮 Try with Demo Data
                        </button>
                    </div>
                </>
            )}

            {progress && (
                <div className="progress-container">
                    <div className="progress-header">
                        <span className={`status status-${progress.status}`}>
                            {(progress.status === 'processing' || progress.status === 'uploading') &&
                                <span className="spinner"></span>
                            }
                            {progress.status.charAt(0).toUpperCase() + progress.status.slice(1)}
                        </span>
                        <span>{Math.round(progress.percentage || 0)}%</span>
                    </div>
                    <div className="progress-bar">
                        <div
                            className="progress-fill"
                            style={{ width: `${progress.percentage || 0}%` }}
                        ></div>
                    </div>
                    <p className="progress-status">{progress.message}</p>

                    {progress.status === 'error' && (
                        <button className="btn btn-secondary" onClick={resetUpload}>
                            Try Again
                        </button>
                    )}
                </div>
            )}
        </div>
    );
}
