import { useState, useEffect, createContext, useContext } from 'react';

// Toast Context
const ToastContext = createContext(null);

export function useToast() {
    return useContext(ToastContext);
}

// Toast types
const TOAST_TYPES = {
    success: { icon: '✓', color: 'var(--color-success)' },
    error: { icon: '✕', color: 'var(--color-error)' },
    warning: { icon: '⚠', color: 'var(--color-warning)' },
    info: { icon: 'ℹ', color: 'var(--color-accent-secondary)' },
};

// Toast Provider Component
export function ToastProvider({ children }) {
    const [toasts, setToasts] = useState([]);

    const addToast = (message, type = 'info', duration = 4000) => {
        const id = Date.now() + Math.random();
        const toast = { id, message, type, duration };

        setToasts(prev => [...prev, toast]);

        if (duration > 0) {
            setTimeout(() => {
                removeToast(id);
            }, duration);
        }

        return id;
    };

    const removeToast = (id) => {
        setToasts(prev => prev.filter(t => t.id !== id));
    };

    const success = (message, duration) => addToast(message, 'success', duration);
    const error = (message, duration) => addToast(message, 'error', duration);
    const warning = (message, duration) => addToast(message, 'warning', duration);
    const info = (message, duration) => addToast(message, 'info', duration);

    return (
        <ToastContext.Provider value={{ addToast, removeToast, success, error, warning, info }}>
            {children}
            <ToastContainer toasts={toasts} onRemove={removeToast} />
        </ToastContext.Provider>
    );
}

// Toast Container
function ToastContainer({ toasts, onRemove }) {
    return (
        <div className="toast-container">
            {toasts.map((toast, index) => (
                <Toast
                    key={toast.id}
                    toast={toast}
                    onRemove={onRemove}
                    style={{ '--toast-index': index }}
                />
            ))}
        </div>
    );
}

// Individual Toast
function Toast({ toast, onRemove, style }) {
    const [isExiting, setIsExiting] = useState(false);
    const config = TOAST_TYPES[toast.type] || TOAST_TYPES.info;

    const handleRemove = () => {
        setIsExiting(true);
        setTimeout(() => onRemove(toast.id), 200);
    };

    return (
        <div
            className={`toast toast-${toast.type} ${isExiting ? 'toast-exit' : ''}`}
            style={{ ...style, '--toast-color': config.color }}
            onClick={handleRemove}
        >
            <span className="toast-icon">{config.icon}</span>
            <span className="toast-message">{toast.message}</span>
            <button className="toast-close" onClick={handleRemove}>×</button>
        </div>
    );
}

export default ToastProvider;
