import { useState, useEffect } from 'react';
import { useToast } from './Toast';
import { Modal } from './Modal';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

/**
 * Database Configuration Component
 * Allows switching between SQLite and PostgreSQL
 */
export default function DatabaseConfig({ jobId, onConfigured }) {
    const [dbType, setDbType] = useState('sqlite');
    const [pgConfig, setPgConfig] = useState({
        host: 'localhost',
        port: 5432,
        database: '',
        user: '',
        password: ''
    });
    const [testing, setTesting] = useState(false);
    const [testResult, setTestResult] = useState(null);
    const [saving, setSaving] = useState(false);
    const { addToast } = useToast();

    const testConnection = async () => {
        setTesting(true);
        setTestResult(null);

        try {
            const params = new URLSearchParams({
                host: pgConfig.host,
                port: pgConfig.port.toString(),
                database: pgConfig.database,
                user: pgConfig.user,
                password: pgConfig.password
            });

            const res = await fetch(`${API_URL}/database/test?${params}`);
            const data = await res.json();

            if (res.ok) {
                setTestResult({ success: true, ...data });
                addToast('Connection successful!', 'success');
            } else {
                setTestResult({ success: false, error: data.detail });
                addToast(data.detail || 'Connection failed', 'error');
            }
        } catch (err) {
            setTestResult({ success: false, error: err.message });
            addToast('Connection test failed', 'error');
        } finally {
            setTesting(false);
        }
    };

    const saveConfig = async () => {
        setSaving(true);

        try {
            const body = {
                db_type: dbType,
                postgresql: dbType === 'postgresql' ? pgConfig : null
            };

            const res = await fetch(`${API_URL}/database/config?job_id=${jobId}`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body)
            });

            if (res.ok) {
                addToast(`Database configured: ${dbType.toUpperCase()}`, 'success');
                onConfigured?.(dbType);
            } else {
                const data = await res.json();
                addToast(data.detail || 'Configuration failed', 'error');
            }
        } catch (err) {
            addToast('Failed to save configuration', 'error');
        } finally {
            setSaving(false);
        }
    };

    return (
        <div className="database-config">
            <h3>🗄️ Database Configuration</h3>

            <div className="db-type-selector">
                <label
                    className={`db-type-option ${dbType === 'sqlite' ? 'active' : ''}`}
                    onClick={() => setDbType('sqlite')}
                >
                    <input
                        type="radio"
                        name="dbType"
                        value="sqlite"
                        checked={dbType === 'sqlite'}
                        onChange={() => setDbType('sqlite')}
                    />
                    <div className="db-type-content">
                        <span className="db-icon">📁</span>
                        <span className="db-name">SQLite</span>
                        <span className="db-desc">File-based, great for development</span>
                    </div>
                </label>

                <label
                    className={`db-type-option ${dbType === 'postgresql' ? 'active' : ''}`}
                    onClick={() => setDbType('postgresql')}
                >
                    <input
                        type="radio"
                        name="dbType"
                        value="postgresql"
                        checked={dbType === 'postgresql'}
                        onChange={() => setDbType('postgresql')}
                    />
                    <div className="db-type-content">
                        <span className="db-icon">🐘</span>
                        <span className="db-name">PostgreSQL</span>
                        <span className="db-desc">Production-grade, scalable</span>
                    </div>
                </label>
            </div>

            {dbType === 'postgresql' && (
                <div className="pg-config">
                    <h4>PostgreSQL Connection</h4>

                    <div className="form-row">
                        <div className="form-group">
                            <label>Host</label>
                            <input
                                type="text"
                                className="prompt-input"
                                value={pgConfig.host}
                                onChange={(e) => setPgConfig({ ...pgConfig, host: e.target.value })}
                                placeholder="localhost"
                            />
                        </div>
                        <div className="form-group" style={{ width: '100px' }}>
                            <label>Port</label>
                            <input
                                type="number"
                                className="prompt-input"
                                value={pgConfig.port}
                                onChange={(e) => setPgConfig({ ...pgConfig, port: parseInt(e.target.value) })}
                            />
                        </div>
                    </div>

                    <div className="form-group">
                        <label>Database Name</label>
                        <input
                            type="text"
                            className="prompt-input"
                            value={pgConfig.database}
                            onChange={(e) => setPgConfig({ ...pgConfig, database: e.target.value })}
                            placeholder="mydb"
                        />
                    </div>

                    <div className="form-row">
                        <div className="form-group">
                            <label>Username</label>
                            <input
                                type="text"
                                className="prompt-input"
                                value={pgConfig.user}
                                onChange={(e) => setPgConfig({ ...pgConfig, user: e.target.value })}
                                placeholder="postgres"
                            />
                        </div>
                        <div className="form-group">
                            <label>Password</label>
                            <input
                                type="password"
                                className="prompt-input"
                                value={pgConfig.password}
                                onChange={(e) => setPgConfig({ ...pgConfig, password: e.target.value })}
                            />
                        </div>
                    </div>

                    <div className="pg-actions">
                        <button
                            className="btn btn-secondary"
                            onClick={testConnection}
                            disabled={testing || !pgConfig.database || !pgConfig.user}
                        >
                            {testing ? '🔄 Testing...' : '🔌 Test Connection'}
                        </button>
                    </div>

                    {testResult && (
                        <div className={`test-result ${testResult.success ? 'success' : 'error'}`}>
                            {testResult.success ? (
                                <>
                                    <p>✅ Connected successfully!</p>
                                    <p className="test-detail">Version: {testResult.version?.substring(0, 50)}...</p>
                                    <p className="test-detail">Tables: {testResult.tables?.length || 0}</p>
                                </>
                            ) : (
                                <p>❌ {testResult.error}</p>
                            )}
                        </div>
                    )}
                </div>
            )}

            <div className="config-actions">
                <button
                    className="btn btn-primary"
                    onClick={saveConfig}
                    disabled={saving || (dbType === 'postgresql' && !testResult?.success)}
                >
                    {saving ? 'Saving...' : '💾 Save Configuration'}
                </button>
            </div>
        </div>
    );
}

/**
 * Database Settings Modal
 */
export function DatabaseSettingsModal({ isOpen, onClose, jobId }) {
    return (
        <Modal isOpen={isOpen} onClose={onClose} title="Database Settings" size="medium">
            <DatabaseConfig jobId={jobId} onConfigured={() => onClose()} />
        </Modal>
    );
}
