import { useState, useEffect } from 'react';
import FileUpload from './components/FileUpload';
import SchemaViewer from './components/SchemaViewer';
import QueryInterface from './components/QueryInterface';
import Dashboard from './components/Dashboard';
import PopulationManager from './components/PopulationManager';
import MessageCenter from './components/MessageCenter';
import ReportBuilder from './components/ReportBuilder';
import DatabaseConfig from './components/DatabaseConfig';
import SmartSuggestions from './components/SmartSuggestions';
import LeadFinder from './components/LeadFinder';
import MemberCRM from './components/MemberCRM';
import { ToastProvider } from './components/Toast';
import { ThemeProvider, ThemeToggle } from './components/Theme';
import { KeyboardProvider, useShortcut } from './components/Keyboard';
import { OnboardingProvider, useOnboarding } from './components/Onboarding';
import { HelpModal, FeedbackForm, VersionInfo } from './components/Help';
import './index.css';
import './styles/enhanced.css';
import './styles/charts.css';
import './styles/final.css';
import './styles/population.css';
import './styles/advanced.css';
import './styles/database.css';
import './styles/query.css';
import './styles/history.css';
import './styles/modern.css';
import './styles/ai-insights.css';
import './styles/lead-finder.css';
import './styles/member-crm.css';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

function AppContent() {
  const [activeTab, setActiveTab] = useState('members');
  const [jobId, setJobId] = useState(null);
  const [schema, setSchema] = useState(null);
  const [populations, setPopulations] = useState([]);
  const [selectedPopulation, setSelectedPopulation] = useState(null);
  const [showHelp, setShowHelp] = useState(false);
  const [showFeedback, setShowFeedback] = useState(false);

  const { startTour } = useOnboarding();

  // Register keyboard shortcuts
  useShortcut('mod+1', () => setActiveTab('upload'), 'Go to Upload');
  useShortcut('mod+2', () => setActiveTab('schema'), 'Go to Schema');
  useShortcut('mod+3', () => setActiveTab('dashboard'), 'Go to Dashboard');
  useShortcut('mod+4', () => setActiveTab('query'), 'Go to Query');
  useShortcut('mod+5', () => setActiveTab('populations'), 'Go to Populations');
  useShortcut('mod+6', () => setActiveTab('reports'), 'Go to Reports');
  useShortcut('mod+7', () => setActiveTab('messages'), 'Go to Messages');
  useShortcut('mod+8', () => setActiveTab('settings'), 'Go to Settings');

  // Load populations when job changes
  useEffect(() => {
    if (jobId) {
      loadPopulations();
    }
  }, [jobId]);

  const loadPopulations = async () => {
    try {
      const res = await fetch(`${API_URL}/populations/${jobId}`);
      const data = await res.json();
      setPopulations(data.populations || []);
    } catch (err) {
      console.error('Failed to load populations');
    }
  };

  const handleUploadComplete = (newJobId, newSchema) => {
    setJobId(newJobId);
    setSchema(newSchema);
    setActiveTab('dashboard');
  };

  const handleSelectPopulation = (pop) => {
    setSelectedPopulation(pop);
    setActiveTab('messages');
  };

  return (
    <div className="app modern-app">
      {/* Modern Header */}
      <header className="modern-header">
        <div className="header-left">
          <div className="brand">
            <div className="brand-icon">
              <svg viewBox="0 0 40 40" fill="none">
                <rect width="40" height="40" rx="10" fill="url(#gradient)" />
                <path d="M12 20L18 14L24 20L30 14" stroke="white" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" />
                <path d="M12 26L18 20L24 26L30 20" stroke="white" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" opacity="0.6" />
                <defs>
                  <linearGradient id="gradient" x1="0" y1="0" x2="40" y2="40">
                    <stop stopColor="#6366f1" />
                    <stop offset="1" stopColor="#8b5cf6" />
                  </linearGradient>
                </defs>
              </svg>
            </div>
            <div className="brand-text">
              <h1>ACA DataHub</h1>
              <span className="brand-badge">
                <span className="gemini-dot"></span>
                Gemini AI
              </span>
            </div>
          </div>
        </div>

        <nav className="modern-nav">
          <button
            className={`nav-item ${activeTab === 'upload' ? 'active' : ''}`}
            onClick={() => setActiveTab('upload')}
          >
            <span className="nav-icon">📤</span>
            <span className="nav-label">Import</span>
          </button>

          <button
            className={`nav-item ${activeTab === 'dashboard' ? 'active' : ''}`}
            onClick={() => setActiveTab('dashboard')}
          >
            <span className="nav-icon">📊</span>
            <span className="nav-label">Insights</span>
          </button>

          <button
            className={`nav-item ${activeTab === 'query' ? 'active' : ''}`}
            onClick={() => setActiveTab('query')}
          >
            <span className="nav-icon">✨</span>
            <span className="nav-label">AI Query</span>
          </button>

          <button
            className={`nav-item ${activeTab === 'leads' ? 'active' : ''}`}
            onClick={() => setActiveTab('leads')}
          >
            <span className="nav-icon">🎯</span>
            <span className="nav-label">Lead Finder</span>
          </button>

          <button
            className={`nav-item ${activeTab === 'members' ? 'active' : ''}`}
            onClick={() => setActiveTab('members')}
          >
            <span className="nav-icon">👥</span>
            <span className="nav-label">Members</span>
          </button>

          <button
            className={`nav-item ${activeTab === 'populations' ? 'active' : ''}`}
            onClick={() => setActiveTab('populations')}
          >
            <span className="nav-icon">📊</span>
            <span className="nav-label">Segments</span>
          </button>

          <button
            className={`nav-item ${activeTab === 'reports' ? 'active' : ''}`}
            onClick={() => setActiveTab('reports')}
          >
            <span className="nav-icon">📈</span>
            <span className="nav-label">Reports</span>
          </button>

          <button
            className={`nav-item ${activeTab === 'messages' ? 'active' : ''}`}
            onClick={() => setActiveTab('messages')}
          >
            <span className="nav-icon">📨</span>
            <span className="nav-label">Outreach</span>
          </button>
        </nav>

        <div className="header-right">
          {jobId && (
            <div className="job-indicator">
              <span className="job-dot"></span>
              Connected
            </div>
          )}
          <button
            className="icon-btn"
            onClick={() => setActiveTab('settings')}
            title="Settings"
          >
            ⚙️
          </button>
          <button
            className="icon-btn"
            onClick={() => setShowHelp(true)}
            title="Help"
          >
            ❓
          </button>
          <ThemeToggle />
        </div>
      </header>

      {/* Main Content */}
      <main className="modern-main">
        {activeTab === 'upload' && (
          <div className="upload-container">
            <FileUpload onUploadComplete={handleUploadComplete} />
            {!jobId && <SmartSuggestions />}
          </div>
        )}

        {activeTab === 'schema' && (
          <SchemaViewer schema={schema} jobId={jobId} />
        )}

        {activeTab === 'dashboard' && (
          <Dashboard jobId={jobId} schema={schema} />
        )}

        {activeTab === 'query' && (
          <QueryInterface
            jobId={jobId}
            schema={schema}
            onSavePopulation={loadPopulations}
          />
        )}

        {activeTab === 'leads' && (
          <LeadFinder
            jobId={jobId}
            schema={schema}
            onSavePopulation={loadPopulations}
          />
        )}

        {activeTab === 'members' && (
          <MemberCRM
            jobId={jobId}
            schema={schema}
          />
        )}

        {activeTab === 'populations' && (
          <PopulationManager
            jobId={jobId}
            schema={schema}
            onSelectPopulation={handleSelectPopulation}
          />
        )}

        {activeTab === 'reports' && (
          <ReportBuilder
            jobId={jobId}
            populations={populations}
            schema={schema}
          />
        )}

        {activeTab === 'messages' && (
          <MessageCenter
            jobId={jobId}
            schema={schema}
            selectedPopulation={selectedPopulation}
          />
        )}

        {activeTab === 'settings' && (
          <div className="settings-page">
            <h2>⚙️ Settings</h2>
            <DatabaseConfig jobId={jobId} />
          </div>
        )}
      </main>

      {/* Footer */}
      <footer className="modern-footer">
        <div className="footer-left">
          <VersionInfo />
        </div>
        <div className="footer-center">
          <span className="footer-brand">ACA DataHub</span>
          <span className="footer-divider">•</span>
          <span>Powered by Google Gemini</span>
        </div>
        <div className="footer-right">
          <button className="footer-link" onClick={() => setShowFeedback(true)}>
            Feedback
          </button>
        </div>
      </footer>

      {/* Modals */}
      <HelpModal isOpen={showHelp} onClose={() => setShowHelp(false)} />
      <FeedbackForm isOpen={showFeedback} onClose={() => setShowFeedback(false)} />
    </div>
  );
}

function App() {
  return (
    <ThemeProvider>
      <ToastProvider>
        <KeyboardProvider>
          <OnboardingProvider>
            <AppContent />
          </OnboardingProvider>
        </KeyboardProvider>
      </ToastProvider>
    </ThemeProvider>
  );
}

export default App;
