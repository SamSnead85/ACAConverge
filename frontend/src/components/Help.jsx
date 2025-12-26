import { useState } from 'react';
import { Modal } from './Modal';

const HELP_SECTIONS = [
    {
        id: 'getting-started',
        title: '🚀 Getting Started',
        content: `
## Quick Start

1. **Upload a File**: Drag and drop your .yxdb file or click to browse
2. **Wait for Conversion**: The file will be converted to a SQL database
3. **View Schema**: Check the table structure in the Schema tab
4. **Query Your Data**: Use natural language or SQL to explore your data

## Demo Mode

Click "Try with Demo Data" to test the app with sample data without uploading a file.
    `
    },
    {
        id: 'querying',
        title: '💬 Querying Data',
        content: `
## Natural Language Queries

Ask questions in plain English:
- "Show all records"
- "Count total records"
- "Show records where sales > 1000"
- "Average of quantity grouped by region"

## Direct SQL

For advanced users, click "SQL Mode" to write SQL directly.

## Tips

- Results are limited to 1000 rows by default
- Use specific column names for better accuracy
- Check the SQL preview before running complex queries
    `
    },
    {
        id: 'export',
        title: '📤 Exporting Data',
        content: `
## Export Options

- **CSV**: Comma-separated values, works with Excel
- **JSON**: For programmatic access
- **SQLite Database**: Download the full database file

## Export from Query Results

After running a query, click the export buttons above the results table.

## Export Full Database

In the Schema tab, click "Download Database" for the complete SQLite file.
    `
    },
    {
        id: 'keyboard',
        title: '⌨️ Keyboard Shortcuts',
        content: `
## Navigation

| Shortcut | Action |
|----------|--------|
| Ctrl/⌘ + 1 | Go to Upload |
| Ctrl/⌘ + 2 | Go to Schema |
| Ctrl/⌘ + 3 | Go to Query |
| Ctrl/⌘ + 4 | Go to History |

## Query

| Shortcut | Action |
|----------|--------|
| Ctrl/⌘ + Enter | Run query |
| Ctrl/⌘ + / | Show shortcuts |
| Escape | Close dialogs |
    `
    },
    {
        id: 'troubleshooting',
        title: '🔧 Troubleshooting',
        content: `
## Common Issues

### File won't upload
- Check that it's a .yxdb file
- Maximum size is 15GB
- Try refreshing the page

### Query returns no results
- Check column names match exactly
- Try a simpler query first
- Use "Show all records" to verify data exists

### Conversion is slow
- Large files take longer
- Check the progress indicator
- Avoid refreshing during conversion

## Getting Help

Contact support or check the documentation for more help.
    `
    }
];

export function HelpModal({ isOpen, onClose }) {
    const [activeSection, setActiveSection] = useState('getting-started');

    if (!isOpen) return null;

    const section = HELP_SECTIONS.find(s => s.id === activeSection);

    return (
        <Modal isOpen={isOpen} onClose={onClose} title="📚 Help & Documentation" size="large">
            <div className="help-container">
                <nav className="help-nav">
                    {HELP_SECTIONS.map(s => (
                        <button
                            key={s.id}
                            className={`help-nav-item ${activeSection === s.id ? 'active' : ''}`}
                            onClick={() => setActiveSection(s.id)}
                        >
                            {s.title}
                        </button>
                    ))}
                </nav>

                <div className="help-content">
                    <div
                        className="help-markdown"
                        dangerouslySetInnerHTML={{ __html: renderMarkdown(section?.content || '') }}
                    />
                </div>
            </div>
        </Modal>
    );
}

// Simple markdown renderer
function renderMarkdown(md) {
    return md
        // Headers
        .replace(/^## (.*$)/gm, '<h3>$1</h3>')
        .replace(/^### (.*$)/gm, '<h4>$1</h4>')
        // Bold
        .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
        // Inline code
        .replace(/`([^`]+)`/g, '<code>$1</code>')
        // Lists
        .replace(/^- (.*$)/gm, '<li>$1</li>')
        .replace(/(<li>.*<\/li>\n?)+/g, '<ul>$&</ul>')
        // Tables (simple)
        .replace(/\|(.+)\|/g, (match) => {
            const cells = match.split('|').filter(c => c.trim());
            if (cells.some(c => /^[-:]+$/.test(c.trim()))) return '';
            const isHeader = cells.every(c => c.endsWith('|'));
            const tag = isHeader ? 'th' : 'td';
            return `<tr>${cells.map(c => `<${tag}>${c.trim()}</${tag}>`).join('')}</tr>`;
        })
        // Paragraphs
        .replace(/\n\n/g, '</p><p>')
        .replace(/^(?!<)(.+)$/gm, '<p>$1</p>')
        // Cleanup
        .replace(/<p><\/p>/g, '')
        .replace(/<p>(<[hul])/g, '$1')
        .replace(/(<\/[hul].*?>)<\/p>/g, '$1');
}

// Feedback Form
export function FeedbackForm({ isOpen, onClose }) {
    const [type, setType] = useState('feedback');
    const [message, setMessage] = useState('');
    const [email, setEmail] = useState('');
    const [submitted, setSubmitted] = useState(false);

    const handleSubmit = (e) => {
        e.preventDefault();
        // In production, send to backend
        console.log('Feedback submitted:', { type, message, email });
        setSubmitted(true);
        setTimeout(() => {
            onClose();
            setSubmitted(false);
            setMessage('');
        }, 2000);
    };

    if (!isOpen) return null;

    return (
        <Modal isOpen={isOpen} onClose={onClose} title="💭 Send Feedback" size="medium">
            {submitted ? (
                <div className="feedback-success">
                    <div className="feedback-success-icon">✅</div>
                    <h3>Thank you!</h3>
                    <p>Your feedback has been received.</p>
                </div>
            ) : (
                <form onSubmit={handleSubmit} className="feedback-form">
                    <div className="form-group">
                        <label>Type</label>
                        <div className="feedback-types">
                            {[
                                { value: 'feedback', label: '💬 Feedback' },
                                { value: 'bug', label: '🐛 Bug Report' },
                                { value: 'feature', label: '✨ Feature Request' }
                            ].map(t => (
                                <button
                                    key={t.value}
                                    type="button"
                                    className={`feedback-type ${type === t.value ? 'active' : ''}`}
                                    onClick={() => setType(t.value)}
                                >
                                    {t.label}
                                </button>
                            ))}
                        </div>
                    </div>

                    <div className="form-group">
                        <label>Message</label>
                        <textarea
                            className="query-input"
                            value={message}
                            onChange={(e) => setMessage(e.target.value)}
                            placeholder="Tell us what's on your mind..."
                            rows={5}
                            required
                        />
                    </div>

                    <div className="form-group">
                        <label>Email (optional)</label>
                        <input
                            type="email"
                            className="prompt-input"
                            value={email}
                            onChange={(e) => setEmail(e.target.value)}
                            placeholder="For follow-up questions"
                        />
                    </div>

                    <div className="modal-actions">
                        <button type="button" className="btn btn-secondary" onClick={onClose}>
                            Cancel
                        </button>
                        <button type="submit" className="btn btn-primary">
                            Send Feedback
                        </button>
                    </div>
                </form>
            )}
        </Modal>
    );
}

// Version Info
export function VersionInfo() {
    return (
        <div className="version-info">
            <span>YXDB Converter v1.0.0</span>
            <span className="separator">•</span>
            <a href="#" className="version-link">Changelog</a>
            <span className="separator">•</span>
            <a href="#" className="version-link">Documentation</a>
        </div>
    );
}

export default HelpModal;
