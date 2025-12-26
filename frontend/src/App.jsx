import { useState, useEffect } from 'react';
import FileUpload from './components/FileUpload';
import SchemaViewer from './components/SchemaViewer';
import QueryInterface from './components/QueryInterface';
import QueryHistory from './components/QueryHistory';
import Dashboard from './components/Dashboard';
import PopulationManager from './components/PopulationManager';
import MessageCenter from './components/MessageCenter';
import ReportBuilder from './components/ReportBuilder';
import DatabaseConfig from './components/DatabaseConfig';
import QueryBuilder from './components/QueryBuilder';
import AudienceInsights from './components/AudienceInsights';
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

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

function AppContent() {
  const [activeTab, setActiveTab] = useState('upload');
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
  useShortcut('mod+3', () => setActiveTab('query'), 'Go to Query');
  useShortcut('mod+4', () => setActiveTab('populations'), 'Go to Populations');
  useShortcut('mod+5', () => setActiveTab('reports'), 'Go to Reports');
  useShortcut('mod+6', () => setActiveTab('messages'), 'Go to Messages');

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
    setActiveTab('schema');
  };

  const handleSelectPopulation = (pop) => {
    setSelectedPopulation(pop);
    setActiveTab('messages');
  };

  const tabs = [
    { id: 'upload', label: '📤 Upload', shortcut: '⌘1' },
    { id: 'schema', label: '📋 Schema', shortcut: '⌘2', disabled: !jobId },
    { id: 'dashboard', label: '📊 Dashboard', shortcut: '⌘3', disabled: !jobId },
    { id: 'query', label: '💬 Query', shortcut: '⌘4', disabled: !jobId },
    { id: 'populations', label: '👥 Populations', shortcut: '⌘5', disabled: !jobId },
    { id: 'reports', label: '📈 Reports', shortcut: '⌘6', disabled: !jobId },
    { id: 'messages', label: '📨 Messages', shortcut: '⌘7', disabled: !jobId },
    { id: 'settings', label: '⚙️ Settings', shortcut: '⌘8' },
  ];

  return (
    <div className="app">
      <header className="header">
        <div className="header-content">
          <div className="logo">
            <div className="logo-icon">⚡</div>
            <div className="logo-text">
              <h1>YXDB Converter</h1>
              <span>Powered by AI</span>
            </div>
          </div>

          <nav className="nav-tabs">
            {tabs.map(tab => (
              <button
                key={tab.id}
                className={`nav-tab ${activeTab === tab.id ? 'active' : ''}`}
                onClick={() => setActiveTab(tab.id)}
                disabled={tab.disabled}
                title={tab.shortcut}
              >
                {tab.label}
              </button>
            ))}
          </nav>

          <div className="header-actions">
            <button
              className="help-btn"
              onClick={startTour}
              title="Start Tour"
            >
              🎯
            </button>
            <button
              className="help-btn"
              onClick={() => setShowHelp(true)}
              title="Help"
            >
              ❓
            </button>
            <button
              className="help-btn"
              onClick={() => setShowFeedback(true)}
              title="Feedback"
            >
              💬
            </button>
            <ThemeToggle />
          </div>
        </div>
      </header>

      <main className="main-content">
        {activeTab === 'upload' && (
          <FileUpload onUploadComplete={handleUploadComplete} />
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

      <footer className="footer">
        <div className="footer-content">
          <VersionInfo />
          <span>Made with ⚡ YXDB Converter</span>
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
