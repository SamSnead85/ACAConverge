import { useState, useEffect } from 'react';
import { useToast } from './Toast';
import { Modal, ConfirmDialog } from './Modal';
import { Spinner } from './Loading';
import DataTable from './DataTable';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

export default function PopulationManager({ jobId, schema, onSelectPopulation }) {
    const [populations, setPopulations] = useState([]);
    const [loading, setLoading] = useState(true);
    const [showCreate, setShowCreate] = useState(false);
    const [showCombine, setShowCombine] = useState(false);
    const [selectedPops, setSelectedPops] = useState([]);
    const [viewingPop, setViewingPop] = useState(null);
    const [popData, setPopData] = useState(null);
    const { addToast } = useToast();

    useEffect(() => {
        if (jobId) loadPopulations();
    }, [jobId]);

    const loadPopulations = async () => {
        try {
            const res = await fetch(`${API_URL}/populations/${jobId}`);
            const data = await res.json();
            setPopulations(data.populations || []);
        } catch (err) {
            addToast('Failed to load populations', 'error');
        } finally {
            setLoading(false);
        }
    };

    const createPopulation = async (name, query, description) => {
        try {
            const res = await fetch(`${API_URL}/populations/${jobId}`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ name, query, description })
            });
            const data = await res.json();
            if (data.population) {
                setPopulations([...populations, data.population]);
                addToast(`Population "${name}" created with ${data.population.count} records`, 'success');
                setShowCreate(false);
            }
        } catch (err) {
            addToast('Failed to create population', 'error');
        }
    };

    const deletePopulation = async (popId) => {
        try {
            await fetch(`${API_URL}/population/${popId}?job_id=${jobId}`, { method: 'DELETE' });
            setPopulations(populations.filter(p => p.id !== popId));
            addToast('Population deleted', 'success');
        } catch (err) {
            addToast('Failed to delete population', 'error');
        }
    };

    const refreshPopulation = async (popId) => {
        try {
            const res = await fetch(`${API_URL}/population/${popId}/refresh?job_id=${jobId}`, {
                method: 'POST'
            });
            const data = await res.json();
            if (data.population) {
                setPopulations(populations.map(p =>
                    p.id === popId ? data.population : p
                ));
                addToast(`Population refreshed: ${data.population.count} records`, 'success');
            }
        } catch (err) {
            addToast('Failed to refresh population', 'error');
        }
    };

    const viewPopulationData = async (pop) => {
        setViewingPop(pop);
        try {
            const res = await fetch(`${API_URL}/population/${pop.id}/data?job_id=${jobId}&limit=100`);
            const data = await res.json();
            setPopData(data);
        } catch (err) {
            addToast('Failed to load population data', 'error');
        }
    };

    const combinePopulations = async (operation, newName) => {
        try {
            const res = await fetch(`${API_URL}/populations/combine?job_id=${jobId}`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    population_ids: selectedPops,
                    operation,
                    new_name: newName
                })
            });
            const data = await res.json();
            if (data.population) {
                setPopulations([...populations, data.population]);
                addToast(`Combined population created: ${data.population.count} records`, 'success');
                setShowCombine(false);
                setSelectedPops([]);
            }
        } catch (err) {
            addToast('Failed to combine populations', 'error');
        }
    };

    const togglePopSelection = (popId) => {
        setSelectedPops(prev =>
            prev.includes(popId)
                ? prev.filter(id => id !== popId)
                : [...prev, popId]
        );
    };

    if (loading) {
        return (
            <div className="card">
                <div className="page-loader">
                    <Spinner size="large" />
                    <p>Loading populations...</p>
                </div>
            </div>
        );
    }

    return (
        <div className="population-manager">
            <div className="population-header">
                <h2>📊 Saved Populations</h2>
                <div className="population-actions">
                    {selectedPops.length >= 2 && (
                        <button className="btn btn-secondary" onClick={() => setShowCombine(true)}>
                            🔗 Combine Selected ({selectedPops.length})
                        </button>
                    )}
                    <button className="btn btn-primary" onClick={() => setShowCreate(true)}>
                        ➕ Create Population
                    </button>
                </div>
            </div>

            {populations.length === 0 ? (
                <div className="empty-state">
                    <div className="empty-state-icon">📋</div>
                    <h3 className="empty-state-title">No Saved Populations</h3>
                    <p>Run a query and save the results as a population to get started.</p>
                </div>
            ) : (
                <div className="population-grid">
                    {populations.map(pop => (
                        <div
                            key={pop.id}
                            className={`population-card ${selectedPops.includes(pop.id) ? 'selected' : ''}`}
                        >
                            <div className="pop-checkbox">
                                <input
                                    type="checkbox"
                                    checked={selectedPops.includes(pop.id)}
                                    onChange={() => togglePopSelection(pop.id)}
                                />
                            </div>
                            <div className="pop-info">
                                <h3>{pop.name}</h3>
                                <p className="pop-description">{pop.description || pop.natural_language}</p>
                                <div className="pop-meta">
                                    <span className="pop-count">{pop.count.toLocaleString()} records</span>
                                    <span className="pop-date">
                                        Created: {new Date(pop.created_at).toLocaleDateString()}
                                    </span>
                                </div>
                                {pop.tags?.length > 0 && (
                                    <div className="pop-tags">
                                        {pop.tags.map(tag => (
                                            <span key={tag} className="pop-tag">{tag}</span>
                                        ))}
                                    </div>
                                )}
                            </div>
                            <div className="pop-actions">
                                <button className="btn btn-sm" onClick={() => viewPopulationData(pop)}>
                                    👁️ View
                                </button>
                                <button className="btn btn-sm" onClick={() => refreshPopulation(pop.id)}>
                                    🔄 Refresh
                                </button>
                                <button
                                    className="btn btn-sm"
                                    onClick={() => onSelectPopulation?.(pop)}
                                >
                                    📨 Message
                                </button>
                                <button
                                    className="btn btn-sm btn-danger"
                                    onClick={() => deletePopulation(pop.id)}
                                >
                                    🗑️
                                </button>
                            </div>
                        </div>
                    ))}
                </div>
            )}

            {/* Create Population Modal */}
            <CreatePopulationModal
                isOpen={showCreate}
                onClose={() => setShowCreate(false)}
                onCreate={createPopulation}
                schema={schema}
            />

            {/* Combine Populations Modal */}
            <CombinePopulationsModal
                isOpen={showCombine}
                onClose={() => setShowCombine(false)}
                onCombine={combinePopulations}
                selectedCount={selectedPops.length}
            />

            {/* View Population Data Modal */}
            {viewingPop && (
                <Modal
                    isOpen={!!viewingPop}
                    onClose={() => { setViewingPop(null); setPopData(null); }}
                    title={`Population: ${viewingPop.name}`}
                    size="large"
                >
                    {popData ? (
                        <DataTable
                            data={popData.records || []}
                            columns={popData.columns || []}
                            pageSize={20}
                        />
                    ) : (
                        <div className="page-loader"><Spinner /></div>
                    )}
                </Modal>
            )}
        </div>
    );
}

function CreatePopulationModal({ isOpen, onClose, onCreate, schema }) {
    const [name, setName] = useState('');
    const [query, setQuery] = useState('SELECT * FROM converted_data');
    const [description, setDescription] = useState('');

    const handleSubmit = (e) => {
        e.preventDefault();
        if (name && query) {
            onCreate(name, query, description);
            setName('');
            setQuery('SELECT * FROM converted_data');
            setDescription('');
        }
    };

    if (!isOpen) return null;

    return (
        <Modal isOpen={isOpen} onClose={onClose} title="Create Population" size="medium">
            <form onSubmit={handleSubmit}>
                <div className="form-group">
                    <label>Population Name *</label>
                    <input
                        type="text"
                        className="prompt-input"
                        value={name}
                        onChange={(e) => setName(e.target.value)}
                        placeholder="e.g., High Value Customers"
                        required
                    />
                </div>

                <div className="form-group">
                    <label>SQL Query *</label>
                    <textarea
                        className="query-input"
                        value={query}
                        onChange={(e) => setQuery(e.target.value)}
                        rows={4}
                        placeholder="SELECT * FROM converted_data WHERE ..."
                        required
                    />
                    <small className="form-help">
                        Available columns: {schema?.map(s => s.name).join(', ')}
                    </small>
                </div>

                <div className="form-group">
                    <label>Description</label>
                    <input
                        type="text"
                        className="prompt-input"
                        value={description}
                        onChange={(e) => setDescription(e.target.value)}
                        placeholder="Optional description"
                    />
                </div>

                <div className="modal-actions">
                    <button type="button" className="btn btn-secondary" onClick={onClose}>
                        Cancel
                    </button>
                    <button type="submit" className="btn btn-primary">
                        Create Population
                    </button>
                </div>
            </form>
        </Modal>
    );
}

function CombinePopulationsModal({ isOpen, onClose, onCombine, selectedCount }) {
    const [operation, setOperation] = useState('union');
    const [name, setName] = useState('');

    const handleSubmit = (e) => {
        e.preventDefault();
        if (name) {
            onCombine(operation, name);
            setName('');
        }
    };

    if (!isOpen) return null;

    return (
        <Modal isOpen={isOpen} onClose={onClose} title="Combine Populations" size="small">
            <form onSubmit={handleSubmit}>
                <div className="form-group">
                    <label>Operation</label>
                    <select
                        className="chart-type-select"
                        value={operation}
                        onChange={(e) => setOperation(e.target.value)}
                        style={{ width: '100%' }}
                    >
                        <option value="union">Union (All records from both)</option>
                        <option value="intersect">Intersect (Only matching records)</option>
                        <option value="exclude">Exclude (First minus others)</option>
                    </select>
                </div>

                <div className="form-group">
                    <label>New Population Name *</label>
                    <input
                        type="text"
                        className="prompt-input"
                        value={name}
                        onChange={(e) => setName(e.target.value)}
                        placeholder="Combined Population"
                        required
                    />
                </div>

                <p className="form-help">
                    Combining {selectedCount} populations
                </p>

                <div className="modal-actions">
                    <button type="button" className="btn btn-secondary" onClick={onClose}>
                        Cancel
                    </button>
                    <button type="submit" className="btn btn-primary">
                        Combine
                    </button>
                </div>
            </form>
        </Modal>
    );
}
