import { useState, useEffect, useCallback } from 'react';
import { useToast } from './Toast';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

/**
 * Member CRM Component
 * Modern, AI-enhanced member search and management interface
 */
export default function MemberCRM({ jobId, schema }) {
    const [searchQuery, setSearchQuery] = useState('');
    const [members, setMembers] = useState([]);
    const [loading, setLoading] = useState(false);
    const [stats, setStats] = useState(null);
    const [showAdvanced, setShowAdvanced] = useState(false);
    const [selectedMember, setSelectedMember] = useState(null);
    const [memberInsights, setMemberInsights] = useState(null);
    const [analyzingMember, setAnalyzingMember] = useState(false);
    const [fieldMappings, setFieldMappings] = useState({});
    const [totalCount, setTotalCount] = useState(0);

    // Advanced search fields
    const [advancedFilters, setAdvancedFilters] = useState({
        member_number: '',
        first_name: '',
        last_name: '',
        ssn_last4: '',
        dob: '',
        email: '',
        phone: '',
        zip_code: '',
        state: '',
        status: '',
        carrier_code: '',
        hix_subscriber_id: '',
        application_id: '',
        effective_date_from: '',
        effective_date_to: '',
    });

    const { addToast } = useToast();

    // Load stats on mount
    useEffect(() => {
        if (jobId) {
            loadStats();
            loadSchema();
        }
    }, [jobId]);

    const loadStats = async () => {
        try {
            const res = await fetch(`${API_URL}/members/stats/${jobId}`);
            const data = await res.json();
            setStats(data);
        } catch (err) {
            console.error('Failed to load stats', err);
        }
    };

    const loadSchema = async () => {
        try {
            const res = await fetch(`${API_URL}/members/schema/${jobId}`);
            const data = await res.json();
            setFieldMappings(data.field_mappings || {});
        } catch (err) {
            console.error('Failed to load schema', err);
        }
    };

    const handleQuickSearch = async () => {
        if (!searchQuery.trim()) return;

        setLoading(true);
        try {
            const res = await fetch(`${API_URL}/members/search`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    job_id: jobId,
                    query: searchQuery,
                    limit: 100
                })
            });
            const data = await res.json();
            setMembers(data.members || []);
            setTotalCount(data.total_count || 0);
            setFieldMappings(data.field_mappings || fieldMappings);
            addToast(`Found ${data.count} members`, 'success');
        } catch (err) {
            addToast('Search failed', 'error');
        } finally {
            setLoading(false);
        }
    };

    const handleAdvancedSearch = async () => {
        setLoading(true);
        try {
            const res = await fetch(`${API_URL}/members/advanced-search`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    job_id: jobId,
                    ...advancedFilters,
                    limit: 100
                })
            });
            const data = await res.json();
            setMembers(data.members || []);
            setTotalCount(data.total_count || 0);
            setFieldMappings(data.field_mappings || fieldMappings);
            addToast(`Found ${data.count} of ${data.total_count} members`, 'success');
        } catch (err) {
            addToast('Search failed', 'error');
        } finally {
            setLoading(false);
        }
    };

    const handleViewMember = async (member) => {
        setSelectedMember(member);
        setMemberInsights(null);
    };

    const handleAnalyzeMember = async () => {
        if (!selectedMember) return;

        setAnalyzingMember(true);
        try {
            const res = await fetch(`${API_URL}/members/ai-analyze`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    job_id: jobId,
                    member_id: String(selectedMember._row_id)
                })
            });
            const data = await res.json();
            if (data.success) {
                setMemberInsights(data.insights);
                addToast('AI analysis complete', 'success');
            } else {
                addToast(data.error || 'Analysis failed', 'error');
            }
        } catch (err) {
            addToast('Analysis failed', 'error');
        } finally {
            setAnalyzingMember(false);
        }
    };

    const clearFilters = () => {
        setAdvancedFilters({
            member_number: '',
            first_name: '',
            last_name: '',
            ssn_last4: '',
            dob: '',
            email: '',
            phone: '',
            zip_code: '',
            state: '',
            status: '',
            carrier_code: '',
            hix_subscriber_id: '',
            application_id: '',
            effective_date_from: '',
            effective_date_to: '',
        });
        setSearchQuery('');
        setMembers([]);
        setTotalCount(0);
    };

    const getMemberDisplayName = (member) => {
        const first = member[fieldMappings.first_name] || '';
        const last = member[fieldMappings.last_name] || '';
        if (first || last) return `${first} ${last}`.trim();
        return `Member #${member._row_id}`;
    };

    const getMemberNumber = (member) => {
        return member[fieldMappings.member_number] || member._row_id;
    };

    // Get columns from members for table display
    const columns = members.length > 0
        ? Object.keys(members[0]).filter(k => !k.startsWith('_')).slice(0, 8)
        : [];

    return (
        <div className="member-crm">
            {/* Header with Stats */}
            <div className="crm-header">
                <div className="header-left">
                    <h2>👥 Member CRM</h2>
                    <span className="gemini-badge">AI-Powered</span>
                </div>
                {stats && (
                    <div className="crm-stats">
                        <div className="stat-item">
                            <span className="stat-value">{stats.total_members?.toLocaleString()}</span>
                            <span className="stat-label">Total Members</span>
                        </div>
                        {Object.entries(stats.by_status || {}).slice(0, 3).map(([status, count]) => (
                            <div key={status} className="stat-item">
                                <span className="stat-value">{count.toLocaleString()}</span>
                                <span className="stat-label">{status}</span>
                            </div>
                        ))}
                    </div>
                )}
            </div>

            {/* Search Section */}
            <div className="search-section">
                {/* Quick Search */}
                <div className="quick-search">
                    <div className="search-input-wrapper">
                        <span className="search-icon">🔍</span>
                        <input
                            type="text"
                            placeholder="Search by name, member #, SSN, phone, or email..."
                            value={searchQuery}
                            onChange={(e) => setSearchQuery(e.target.value)}
                            onKeyDown={(e) => e.key === 'Enter' && handleQuickSearch()}
                            className="search-input"
                        />
                        <button
                            className="btn btn-primary search-btn"
                            onClick={handleQuickSearch}
                            disabled={loading || !searchQuery.trim()}
                        >
                            {loading ? '🔄' : '➜'} Search
                        </button>
                    </div>
                    <button
                        className="btn btn-secondary toggle-advanced"
                        onClick={() => setShowAdvanced(!showAdvanced)}
                    >
                        {showAdvanced ? '▲ Hide' : '▼ Advanced'} Search
                    </button>
                </div>

                {/* Advanced Search Panel */}
                {showAdvanced && (
                    <div className="advanced-search">
                        <div className="advanced-header">
                            <h3>Advanced Search</h3>
                            <button className="btn btn-sm" onClick={clearFilters}>
                                Clear All
                            </button>
                        </div>

                        <div className="filter-grid">
                            {/* Identity Section */}
                            <div className="filter-group">
                                <label>Member Number</label>
                                <input
                                    type="text"
                                    placeholder="e.g. 8035Z7"
                                    value={advancedFilters.member_number}
                                    onChange={(e) => setAdvancedFilters({ ...advancedFilters, member_number: e.target.value })}
                                />
                            </div>
                            <div className="filter-group">
                                <label>First Name</label>
                                <input
                                    type="text"
                                    placeholder="First Name"
                                    value={advancedFilters.first_name}
                                    onChange={(e) => setAdvancedFilters({ ...advancedFilters, first_name: e.target.value })}
                                />
                            </div>
                            <div className="filter-group">
                                <label>Last Name</label>
                                <input
                                    type="text"
                                    placeholder="Last Name"
                                    value={advancedFilters.last_name}
                                    onChange={(e) => setAdvancedFilters({ ...advancedFilters, last_name: e.target.value })}
                                />
                            </div>
                            <div className="filter-group">
                                <label>SSN (Last 4)</label>
                                <input
                                    type="text"
                                    placeholder="XXXX"
                                    maxLength={4}
                                    value={advancedFilters.ssn_last4}
                                    onChange={(e) => setAdvancedFilters({ ...advancedFilters, ssn_last4: e.target.value })}
                                />
                            </div>
                            <div className="filter-group">
                                <label>Date of Birth</label>
                                <input
                                    type="date"
                                    value={advancedFilters.dob}
                                    onChange={(e) => setAdvancedFilters({ ...advancedFilters, dob: e.target.value })}
                                />
                            </div>

                            {/* Contact Section */}
                            <div className="filter-group">
                                <label>Email</label>
                                <input
                                    type="email"
                                    placeholder="email@example.com"
                                    value={advancedFilters.email}
                                    onChange={(e) => setAdvancedFilters({ ...advancedFilters, email: e.target.value })}
                                />
                            </div>
                            <div className="filter-group">
                                <label>Phone</label>
                                <input
                                    type="tel"
                                    placeholder="(555) 555-5555"
                                    value={advancedFilters.phone}
                                    onChange={(e) => setAdvancedFilters({ ...advancedFilters, phone: e.target.value })}
                                />
                            </div>
                            <div className="filter-group">
                                <label>ZIP Code</label>
                                <input
                                    type="text"
                                    placeholder="12345"
                                    maxLength={10}
                                    value={advancedFilters.zip_code}
                                    onChange={(e) => setAdvancedFilters({ ...advancedFilters, zip_code: e.target.value })}
                                />
                            </div>
                            <div className="filter-group">
                                <label>State</label>
                                <input
                                    type="text"
                                    placeholder="GA, FL, TX..."
                                    maxLength={2}
                                    value={advancedFilters.state}
                                    onChange={(e) => setAdvancedFilters({ ...advancedFilters, state: e.target.value.toUpperCase() })}
                                />
                            </div>

                            {/* Enrollment Section */}
                            <div className="filter-group">
                                <label>Status</label>
                                <input
                                    type="text"
                                    placeholder="Active, Pending..."
                                    value={advancedFilters.status}
                                    onChange={(e) => setAdvancedFilters({ ...advancedFilters, status: e.target.value })}
                                />
                            </div>
                            <div className="filter-group">
                                <label>Carrier Code</label>
                                <input
                                    type="text"
                                    placeholder="Carrier"
                                    value={advancedFilters.carrier_code}
                                    onChange={(e) => setAdvancedFilters({ ...advancedFilters, carrier_code: e.target.value })}
                                />
                            </div>
                            <div className="filter-group">
                                <label>HIX Subscriber ID</label>
                                <input
                                    type="text"
                                    placeholder="HIX ID"
                                    value={advancedFilters.hix_subscriber_id}
                                    onChange={(e) => setAdvancedFilters({ ...advancedFilters, hix_subscriber_id: e.target.value })}
                                />
                            </div>
                            <div className="filter-group">
                                <label>Application ID</label>
                                <input
                                    type="text"
                                    placeholder="App ID"
                                    value={advancedFilters.application_id}
                                    onChange={(e) => setAdvancedFilters({ ...advancedFilters, application_id: e.target.value })}
                                />
                            </div>

                            {/* Date Ranges */}
                            <div className="filter-group">
                                <label>Effective Date From</label>
                                <input
                                    type="date"
                                    value={advancedFilters.effective_date_from}
                                    onChange={(e) => setAdvancedFilters({ ...advancedFilters, effective_date_from: e.target.value })}
                                />
                            </div>
                            <div className="filter-group">
                                <label>Effective Date To</label>
                                <input
                                    type="date"
                                    value={advancedFilters.effective_date_to}
                                    onChange={(e) => setAdvancedFilters({ ...advancedFilters, effective_date_to: e.target.value })}
                                />
                            </div>
                        </div>

                        <div className="advanced-actions">
                            <button
                                className="btn btn-primary"
                                onClick={handleAdvancedSearch}
                                disabled={loading}
                            >
                                {loading ? '🔄 Searching...' : '🔍 Search'}
                            </button>
                        </div>
                    </div>
                )}
            </div>

            {/* Results Section */}
            <div className="crm-content">
                {/* Results Table */}
                <div className={`results-panel ${selectedMember ? 'with-detail' : ''}`}>
                    {members.length > 0 ? (
                        <>
                            <div className="results-header">
                                <h3>📋 Results ({members.length} of {totalCount.toLocaleString()})</h3>
                            </div>
                            <div className="table-scroll">
                                <table className="members-table">
                                    <thead>
                                        <tr>
                                            {columns.map(col => (
                                                <th key={col}>{col.replace(/_/g, ' ')}</th>
                                            ))}
                                            <th>Actions</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {members.map((member, i) => (
                                            <tr
                                                key={member._row_id || i}
                                                className={selectedMember?._row_id === member._row_id ? 'selected' : ''}
                                                onClick={() => handleViewMember(member)}
                                            >
                                                {columns.map(col => (
                                                    <td key={col}>{String(member[col] || '')}</td>
                                                ))}
                                                <td>
                                                    <button
                                                        className="btn btn-sm btn-secondary"
                                                        onClick={(e) => {
                                                            e.stopPropagation();
                                                            handleViewMember(member);
                                                        }}
                                                    >
                                                        View
                                                    </button>
                                                </td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        </>
                    ) : !loading ? (
                        <div className="empty-state">
                            <div className="empty-icon">🔍</div>
                            <h3>Search for Members</h3>
                            <p>Use the search bar above to find members by name, ID, or other attributes.</p>
                        </div>
                    ) : (
                        <div className="loading-state">
                            <div className="gemini-loader">
                                <span></span><span></span><span></span><span></span>
                            </div>
                            <p>Searching members...</p>
                        </div>
                    )}
                </div>

                {/* Member Detail Panel */}
                {selectedMember && (
                    <div className="member-detail">
                        <div className="detail-header">
                            <h3>{getMemberDisplayName(selectedMember)}</h3>
                            <button
                                className="close-btn"
                                onClick={() => setSelectedMember(null)}
                            >
                                ✕
                            </button>
                        </div>

                        <div className="detail-content">
                            {/* Member Info Cards */}
                            <div className="info-section">
                                <h4>📋 Member Information</h4>
                                <div className="info-grid">
                                    {Object.entries(selectedMember)
                                        .filter(([k]) => !k.startsWith('_'))
                                        .slice(0, 12)
                                        .map(([key, value]) => (
                                            <div key={key} className="info-item">
                                                <span className="info-label">{key.replace(/_/g, ' ')}</span>
                                                <span className="info-value">{String(value || '—')}</span>
                                            </div>
                                        ))}
                                </div>
                            </div>

                            {/* AI Analysis Section */}
                            <div className="ai-section">
                                <div className="ai-header">
                                    <h4>✨ AI Analysis</h4>
                                    <button
                                        className="btn btn-primary btn-sm"
                                        onClick={handleAnalyzeMember}
                                        disabled={analyzingMember}
                                    >
                                        {analyzingMember ? '🔄 Analyzing...' : '✨ Analyze'}
                                    </button>
                                </div>

                                {memberInsights ? (
                                    <div className="insights-content">
                                        {memberInsights.summary && (
                                            <p className="insight-summary">{memberInsights.summary}</p>
                                        )}

                                        {memberInsights.eligibility && (
                                            <div className={`eligibility-badge ${memberInsights.eligibility}`}>
                                                {memberInsights.eligibility === 'eligible' && '✅ Likely Eligible'}
                                                {memberInsights.eligibility === 'needs_review' && '⚠️ Needs Review'}
                                                {memberInsights.eligibility === 'potential_issues' && '❌ Potential Issues'}
                                            </div>
                                        )}

                                        {memberInsights.recommended_actions?.length > 0 && (
                                            <div className="insight-block">
                                                <strong>Recommended Actions:</strong>
                                                <ul>
                                                    {memberInsights.recommended_actions.map((action, i) => (
                                                        <li key={i}>{action}</li>
                                                    ))}
                                                </ul>
                                            </div>
                                        )}

                                        {memberInsights.outreach && (
                                            <div className="insight-block">
                                                <strong>Outreach Recommendation:</strong>
                                                <p>
                                                    📞 {memberInsights.outreach.channel} — {memberInsights.outreach.timing}
                                                </p>
                                            </div>
                                        )}
                                    </div>
                                ) : (
                                    <p className="ai-placeholder">
                                        Click "Analyze" to get AI-powered insights about this member's eligibility, risk factors, and recommended actions.
                                    </p>
                                )}
                            </div>

                            {/* Quick Actions */}
                            <div className="actions-section">
                                <h4>⚡ Quick Actions</h4>
                                <div className="action-buttons">
                                    <button className="btn btn-secondary">📧 Send Email</button>
                                    <button className="btn btn-secondary">📞 Log Call</button>
                                    <button className="btn btn-secondary">📝 Add Note</button>
                                    <button className="btn btn-secondary">📊 View History</button>
                                </div>
                            </div>
                        </div>
                    </div>
                )}
            </div>
        </div>
    );
}
