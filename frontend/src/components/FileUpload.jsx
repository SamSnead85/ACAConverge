import { useState, useRef, useCallback } from 'react';

export default function FileUpload({ onUploadComplete, onStartDemo }) {
    const [isDragging, setIsDragging] = useState(false);
    const [file, setFile] = useState(null);
    const [uploading, setUploading] = useState(false);
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
            setFile(droppedFile);
            setError(null);
        }
    }, []);

    const handleFileSelect = useCallback((e) => {
        const selectedFile = e.target.files[0];
        if (selectedFile) {
            setFile(selectedFile);
            setError(null);
        }
    }, []);

    const handleUpload = async () => {
        if (!file) return;

        setUploading(true);
        setError(null);
        setProgress({ status: 'uploading', percentage: 0, message: 'Uploading file...' });

        try {
            const formData = new FormData();
            formData.append('file', file);

            // Check if it's a real yxdb file or use mock
            const useMock = !file.name.toLowerCase().endsWith('.yxdb');

            const response = await fetch(
                `${import.meta.env.VITE_API_URL || 'http://localhost:8000/api'}/upload?use_mock=${useMock}`,
                {
                    method: 'POST',
                    body: formData,
                }
            );

            if (!response.ok) {
                throw new Error('Upload failed');
            }

            const data = await response.json();
            setProgress({ status: 'processing', percentage: 10, message: 'Processing file...' });

            // Poll for progress
            pollProgress(data.job_id);
        } catch (err) {
            setError(err.message);
            setUploading(false);
            setProgress(null);
        }
    };

    const pollProgress = async (jobId) => {
        try {
            const response = await fetch(
                `${import.meta.env.VITE_API_URL || 'http://localhost:8000/api'}/conversion/status/${jobId}`
            );
            const data = await response.json();

            setProgress(data.progress);

            if (data.progress.status === 'completed') {
                setUploading(false);
                onUploadComplete(jobId, data.schema);
            } else if (data.progress.status === 'error') {
                setError(data.progress.error || 'Conversion failed');
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
        setProgress({ status: 'processing', percentage: 0, message: 'Starting demo...' });

        try {
            const response = await fetch(
                `${import.meta.env.VITE_API_URL || 'http://localhost:8000/api'}/upload/demo`,
                { method: 'POST' }
            );

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

    return (
        <div className="card">
            <div className="card-header">
                <h2 className="card-title">Upload YXDB File</h2>
                <p className="card-description">
                    Upload your Alteryx .yxdb file to convert it to a SQL database
                </p>
            </div>

            {error && (
                <div className="alert alert-error">
                    {error}
                </div>
            )}

            {!progress && (
                <>
                    <div
                        className={`upload-zone ${isDragging ? 'dragover' : ''}`}
                        onDragOver={handleDragOver}
                        onDragLeave={handleDragLeave}
                        onDrop={handleDrop}
                        onClick={() => fileInputRef.current?.click()}
                    >
                        <div className="upload-icon">📁</div>
                        <h3 className="upload-title">
                            {file ? file.name : 'Drop your .yxdb file here'}
                        </h3>
                        <p className="upload-subtitle">
                            {file
                                ? formatFileSize(file.size)
                                : 'or click to browse'}
                        </p>
                        <div className="upload-hint">
                            Supported format: .yxdb (Alteryx Database Files) • Max size: 10GB+
                        </div>
                        <input
                            type="file"
                            ref={fileInputRef}
                            onChange={handleFileSelect}
                            accept=".yxdb"
                            style={{ display: 'none' }}
                        />
                    </div>

                    <div style={{ display: 'flex', gap: '1rem', marginTop: '1.5rem', justifyContent: 'center' }}>
                        <button
                            className="btn btn-primary"
                            onClick={handleUpload}
                            disabled={!file || uploading}
                        >
                            {uploading ? 'Converting...' : 'Convert to SQL'}
                        </button>
                        <button
                            className="btn btn-secondary"
                            onClick={handleDemo}
                            disabled={uploading}
                        >
                            Try with Demo Data
                        </button>
                    </div>
                </>
            )}

            {progress && (
                <div className="progress-container">
                    <div className="progress-header">
                        <span className={`status status-${progress.status}`}>
                            {progress.status === 'processing' && <span className="spinner"></span>}
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
                </div>
            )}
        </div>
    );
}
