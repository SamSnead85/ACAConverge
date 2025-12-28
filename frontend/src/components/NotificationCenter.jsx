import { useState, useEffect, useRef, createContext, useContext } from 'react';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';
const WS_URL = API_URL.replace('http', 'ws').replace('/api', '');

// Notification Context
const NotificationContext = createContext(null);

export function useNotifications() {
    return useContext(NotificationContext);
}

export function NotificationProvider({ children, userId }) {
    const [notifications, setNotifications] = useState([]);
    const [unreadCount, setUnreadCount] = useState(0);
    const wsRef = useRef(null);

    useEffect(() => {
        if (!userId) return;

        // Connect to WebSocket
        const ws = new WebSocket(`${WS_URL}/ws/notifications/${userId}`);
        wsRef.current = ws;

        ws.onopen = () => {
            console.log('Notification WebSocket connected');
        };

        ws.onmessage = (event) => {
            const data = JSON.parse(event.data);

            if (data.type === 'notifications_state') {
                setNotifications(data.notifications);
                setUnreadCount(data.unread_count);
            } else if (data.type === 'notification') {
                setNotifications(prev => [data.notification, ...prev]);
                setUnreadCount(prev => prev + 1);
            } else if (data.type === 'unread_count') {
                setUnreadCount(data.count);
            }
        };

        ws.onclose = () => {
            console.log('Notification WebSocket disconnected');
            // Attempt reconnect after 5s
            setTimeout(() => {
                if (wsRef.current?.readyState === WebSocket.CLOSED) {
                    // Reconnect logic would go here
                }
            }, 5000);
        };

        return () => {
            ws.close();
        };
    }, [userId]);

    const markRead = (notificationId = null) => {
        if (wsRef.current?.readyState === WebSocket.OPEN) {
            wsRef.current.send(JSON.stringify({
                type: 'mark_read',
                notification_id: notificationId
            }));
        }

        if (notificationId) {
            setNotifications(prev =>
                prev.map(n => n.id === notificationId ? { ...n, read: true } : n)
            );
            setUnreadCount(prev => Math.max(0, prev - 1));
        } else {
            setNotifications(prev => prev.map(n => ({ ...n, read: true })));
            setUnreadCount(0);
        }
    };

    return (
        <NotificationContext.Provider value={{ notifications, unreadCount, markRead }}>
            {children}
        </NotificationContext.Provider>
    );
}

// Notification Center Component
export default function NotificationCenter() {
    const [isOpen, setIsOpen] = useState(false);
    const { notifications = [], unreadCount = 0, markRead } = useNotifications() || {};
    const dropdownRef = useRef(null);

    // Close on outside click
    useEffect(() => {
        const handleClickOutside = (e) => {
            if (dropdownRef.current && !dropdownRef.current.contains(e.target)) {
                setIsOpen(false);
            }
        };

        document.addEventListener('mousedown', handleClickOutside);
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, []);

    const getIcon = (type) => {
        switch (type) {
            case 'mention': return '💬';
            case 'query': return '🔍';
            case 'report': return '📊';
            case 'campaign': return '📧';
            case 'system': return '⚙️';
            default: return '🔔';
        }
    };

    const formatTime = (timestamp) => {
        const date = new Date(timestamp);
        const now = new Date();
        const diff = now - date;

        if (diff < 60000) return 'Just now';
        if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`;
        if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`;
        return date.toLocaleDateString();
    };

    return (
        <div className="notification-center" ref={dropdownRef}>
            <button
                className="notification-trigger"
                onClick={() => setIsOpen(!isOpen)}
            >
                🔔
                {unreadCount > 0 && (
                    <span className="notification-badge">{unreadCount > 99 ? '99+' : unreadCount}</span>
                )}
            </button>

            {isOpen && (
                <div className="notification-dropdown">
                    <div className="notification-header">
                        <h3>Notifications</h3>
                        {unreadCount > 0 && (
                            <button
                                className="mark-all-read"
                                onClick={() => markRead()}
                            >
                                Mark all read
                            </button>
                        )}
                    </div>

                    <div className="notification-list">
                        {notifications.length === 0 ? (
                            <div className="notification-empty">
                                <span className="empty-icon">🔔</span>
                                <p>No notifications yet</p>
                            </div>
                        ) : (
                            notifications.map(notification => (
                                <div
                                    key={notification.id}
                                    className={`notification-item ${notification.read ? 'read' : 'unread'}`}
                                    onClick={() => !notification.read && markRead(notification.id)}
                                >
                                    <span className="notification-icon">{getIcon(notification.type)}</span>
                                    <div className="notification-content">
                                        <p className="notification-title">{notification.title}</p>
                                        {notification.body && (
                                            <p className="notification-body">{notification.body}</p>
                                        )}
                                        <span className="notification-time">{formatTime(notification.created_at)}</span>
                                    </div>
                                    {!notification.read && <span className="unread-dot" />}
                                </div>
                            ))
                        )}
                    </div>
                </div>
            )}
        </div>
    );
}

// Activity Feed Component
export function ActivityFeed({ workspaceId }) {
    const [activities, setActivities] = useState([]);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        if (!workspaceId) return;

        const fetchActivities = async () => {
            try {
                const res = await fetch(`${API_URL}/workspace/${workspaceId}/activity`);
                const data = await res.json();
                setActivities(data.activities || []);
            } catch (err) {
                console.error('Failed to fetch activities:', err);
            } finally {
                setLoading(false);
            }
        };

        fetchActivities();
    }, [workspaceId]);

    const getActionIcon = (action) => {
        if (action.includes('query')) return '🔍';
        if (action.includes('population')) return '👥';
        if (action.includes('report')) return '📊';
        if (action.includes('message')) return '📧';
        if (action.includes('import')) return '📤';
        return '📝';
    };

    const formatTime = (timestamp) => {
        const date = new Date(timestamp);
        const now = new Date();
        const diff = now - date;

        if (diff < 60000) return 'Just now';
        if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`;
        if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`;
        return date.toLocaleString();
    };

    if (loading) {
        return (
            <div className="activity-feed loading">
                <div className="activity-skeleton" />
                <div className="activity-skeleton" />
                <div className="activity-skeleton" />
            </div>
        );
    }

    return (
        <div className="activity-feed">
            <h3>Activity Feed</h3>
            <div className="activity-list">
                {activities.length === 0 ? (
                    <div className="activity-empty">
                        <p>No activity yet</p>
                    </div>
                ) : (
                    activities.map(activity => (
                        <div key={activity.id} className="activity-item">
                            <span className="activity-icon">{getActionIcon(activity.action)}</span>
                            <div className="activity-content">
                                <span className="activity-user">{activity.user_name}</span>
                                <span className="activity-action">{activity.action}</span>
                                {activity.details?.name && (
                                    <span className="activity-target">{activity.details.name}</span>
                                )}
                            </div>
                            <span className="activity-time">{formatTime(activity.timestamp)}</span>
                        </div>
                    ))
                )}
            </div>
        </div>
    );
}

// Presence Indicator Component
export function PresenceIndicator({ workspaceId }) {
    const [users, setUsers] = useState([]);
    const wsRef = useRef(null);

    useEffect(() => {
        if (!workspaceId) return;

        // Fetch initial presence
        const fetchPresence = async () => {
            try {
                const res = await fetch(`${API_URL}/workspace/${workspaceId}/presence`);
                const data = await res.json();
                setUsers(data.users || []);
            } catch (err) {
                console.error('Failed to fetch presence:', err);
            }
        };

        fetchPresence();
    }, [workspaceId]);

    if (users.length === 0) return null;

    return (
        <div className="presence-indicator">
            <div className="presence-avatars">
                {users.slice(0, 5).map((user, i) => (
                    <div
                        key={user.user_id}
                        className="presence-avatar"
                        title={user.user_name}
                        style={{ zIndex: users.length - i }}
                    >
                        {user.user_name.charAt(0).toUpperCase()}
                    </div>
                ))}
                {users.length > 5 && (
                    <div className="presence-more">+{users.length - 5}</div>
                )}
            </div>
            <span className="presence-label">{users.length} online</span>
        </div>
    );
}
