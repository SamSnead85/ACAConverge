import { useState } from 'react';

// Modal Component
export function Modal({ isOpen, onClose, title, children, size = 'medium' }) {
    if (!isOpen) return null;

    const handleBackdropClick = (e) => {
        if (e.target === e.currentTarget) {
            onClose();
        }
    };

    return (
        <div className="modal-backdrop" onClick={handleBackdropClick}>
            <div className={`modal modal-${size}`}>
                <div className="modal-header">
                    <h3 className="modal-title">{title}</h3>
                    <button className="modal-close" onClick={onClose}>×</button>
                </div>
                <div className="modal-content">
                    {children}
                </div>
            </div>
        </div>
    );
}

// Confirm Dialog
export function ConfirmDialog({ isOpen, onClose, onConfirm, title, message, confirmText = 'Confirm', cancelText = 'Cancel', variant = 'default' }) {
    if (!isOpen) return null;

    const handleConfirm = () => {
        onConfirm();
        onClose();
    };

    return (
        <Modal isOpen={isOpen} onClose={onClose} title={title} size="small">
            <p className="confirm-message">{message}</p>
            <div className="modal-actions">
                <button className="btn btn-secondary" onClick={onClose}>
                    {cancelText}
                </button>
                <button
                    className={`btn ${variant === 'danger' ? 'btn-danger' : 'btn-primary'}`}
                    onClick={handleConfirm}
                >
                    {confirmText}
                </button>
            </div>
        </Modal>
    );
}

// Prompt Dialog
export function PromptDialog({ isOpen, onClose, onSubmit, title, message, placeholder = '', defaultValue = '' }) {
    const [value, setValue] = useState(defaultValue);

    if (!isOpen) return null;

    const handleSubmit = (e) => {
        e.preventDefault();
        onSubmit(value);
        onClose();
    };

    return (
        <Modal isOpen={isOpen} onClose={onClose} title={title} size="small">
            <form onSubmit={handleSubmit}>
                <p className="prompt-message">{message}</p>
                <input
                    type="text"
                    className="prompt-input"
                    value={value}
                    onChange={(e) => setValue(e.target.value)}
                    placeholder={placeholder}
                    autoFocus
                />
                <div className="modal-actions">
                    <button type="button" className="btn btn-secondary" onClick={onClose}>
                        Cancel
                    </button>
                    <button type="submit" className="btn btn-primary">
                        Submit
                    </button>
                </div>
            </form>
        </Modal>
    );
}

export default Modal;
