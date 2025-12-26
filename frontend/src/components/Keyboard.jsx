import { useEffect, useCallback, createContext, useContext, useState } from 'react';

// Keyboard Shortcuts Context
const KeyboardContext = createContext(null);

export function useKeyboard() {
    return useContext(KeyboardContext);
}

// Default shortcuts
const DEFAULT_SHORTCUTS = {
    'mod+k': { description: 'Open command palette', action: null },
    'mod+/': { description: 'Show keyboard shortcuts', action: null },
    'mod+enter': { description: 'Run query', action: null },
    'mod+s': { description: 'Export results', action: null },
    'mod+1': { description: 'Go to Upload tab', action: null },
    'mod+2': { description: 'Go to Schema tab', action: null },
    'mod+3': { description: 'Go to Query tab', action: null },
    'mod+4': { description: 'Go to History tab', action: null },
    'escape': { description: 'Close modal/dialog', action: null },
};

export function KeyboardProvider({ children }) {
    const [shortcuts, setShortcuts] = useState(DEFAULT_SHORTCUTS);
    const [showHelp, setShowHelp] = useState(false);

    const registerShortcut = useCallback((key, action, description) => {
        setShortcuts(prev => ({
            ...prev,
            [key]: { description: description || prev[key]?.description, action }
        }));
    }, []);

    const unregisterShortcut = useCallback((key) => {
        setShortcuts(prev => ({
            ...prev,
            [key]: { ...prev[key], action: null }
        }));
    }, []);

    useEffect(() => {
        const handleKeyDown = (e) => {
            const isMod = e.metaKey || e.ctrlKey;
            const key = e.key.toLowerCase();

            let shortcutKey = '';
            if (isMod) shortcutKey += 'mod+';
            if (e.shiftKey) shortcutKey += 'shift+';
            if (e.altKey) shortcutKey += 'alt+';
            shortcutKey += key;

            // Check for matching shortcut
            if (shortcuts[shortcutKey]?.action) {
                e.preventDefault();
                shortcuts[shortcutKey].action();
                return;
            }

            // Special: Show help with mod+/
            if (shortcutKey === 'mod+/') {
                e.preventDefault();
                setShowHelp(prev => !prev);
                return;
            }

            // Special: Escape closes help
            if (key === 'escape' && showHelp) {
                setShowHelp(false);
                return;
            }
        };

        window.addEventListener('keydown', handleKeyDown);
        return () => window.removeEventListener('keydown', handleKeyDown);
    }, [shortcuts, showHelp]);

    return (
        <KeyboardContext.Provider value={{
            registerShortcut,
            unregisterShortcut,
            shortcuts,
            showHelp,
            setShowHelp
        }}>
            {children}
            {showHelp && <KeyboardShortcutsHelp onClose={() => setShowHelp(false)} />}
        </KeyboardContext.Provider>
    );
}

// Keyboard Shortcuts Help Modal
function KeyboardShortcutsHelp({ onClose }) {
    const { shortcuts } = useKeyboard();
    const isMac = navigator.platform.toUpperCase().indexOf('MAC') >= 0;
    const modKey = isMac ? '⌘' : 'Ctrl';

    const formatKey = (key) => {
        return key
            .replace('mod', modKey)
            .replace('+', ' + ')
            .replace('enter', '↵')
            .replace('escape', 'Esc')
            .toUpperCase();
    };

    return (
        <div className="modal-backdrop" onClick={onClose}>
            <div className="modal modal-medium keyboard-help" onClick={e => e.stopPropagation()}>
                <div className="modal-header">
                    <h3 className="modal-title">⌨️ Keyboard Shortcuts</h3>
                    <button className="modal-close" onClick={onClose}>×</button>
                </div>
                <div className="modal-content">
                    <div className="shortcuts-list">
                        {Object.entries(shortcuts).map(([key, { description }]) => (
                            <div key={key} className="shortcut-item">
                                <span className="shortcut-description">{description}</span>
                                <kbd className="shortcut-key">{formatKey(key)}</kbd>
                            </div>
                        ))}
                    </div>
                </div>
            </div>
        </div>
    );
}

// Hook for registering shortcuts in components
export function useShortcut(key, callback, description) {
    const { registerShortcut, unregisterShortcut } = useKeyboard();

    useEffect(() => {
        registerShortcut(key, callback, description);
        return () => unregisterShortcut(key);
    }, [key, callback, description, registerShortcut, unregisterShortcut]);
}

export default KeyboardProvider;
