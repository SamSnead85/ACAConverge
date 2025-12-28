import { useState, useEffect, createContext, useContext } from 'react';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

// Auth Context
const AuthContext = createContext(null);

export function useAuth() {
    return useContext(AuthContext);
}

export function AuthProvider({ children }) {
    const [user, setUser] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        // Check for existing token on mount
        const token = localStorage.getItem('accessToken');
        if (token) {
            fetchCurrentUser(token);
        } else {
            setLoading(false);
        }
    }, []);

    const fetchCurrentUser = async (token) => {
        try {
            const res = await fetch(`${API_URL}/auth/me`, {
                headers: { Authorization: `Bearer ${token}` }
            });
            if (res.ok) {
                const data = await res.json();
                setUser(data);
            } else {
                // Token invalid, clear it
                localStorage.removeItem('accessToken');
                localStorage.removeItem('refreshToken');
            }
        } catch (err) {
            console.error('Failed to fetch user:', err);
        } finally {
            setLoading(false);
        }
    };

    const login = async (email, password, totpCode = null) => {
        setError(null);
        try {
            const res = await fetch(`${API_URL}/auth/login`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ email, password, totp_code: totpCode })
            });
            const data = await res.json();

            if (!res.ok) {
                throw new Error(data.detail || 'Login failed');
            }

            if (data.requires_2fa) {
                return { requires2FA: true };
            }

            localStorage.setItem('accessToken', data.access_token);
            localStorage.setItem('refreshToken', data.refresh_token);
            setUser(data.user);
            return { success: true };
        } catch (err) {
            setError(err.message);
            throw err;
        }
    };

    const register = async (email, password, firstName = '', lastName = '') => {
        setError(null);
        try {
            const res = await fetch(`${API_URL}/auth/register`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    email,
                    password,
                    first_name: firstName,
                    last_name: lastName
                })
            });
            const data = await res.json();

            if (!res.ok) {
                throw new Error(data.detail || 'Registration failed');
            }

            return { success: true, message: data.message };
        } catch (err) {
            setError(err.message);
            throw err;
        }
    };

    const logout = async () => {
        try {
            const token = localStorage.getItem('accessToken');
            if (token) {
                await fetch(`${API_URL}/auth/logout`, {
                    method: 'POST',
                    headers: { Authorization: `Bearer ${token}` }
                });
            }
        } catch (err) {
            console.error('Logout error:', err);
        } finally {
            localStorage.removeItem('accessToken');
            localStorage.removeItem('refreshToken');
            setUser(null);
        }
    };

    const refreshToken = async () => {
        const refresh = localStorage.getItem('refreshToken');
        if (!refresh) return false;

        try {
            const res = await fetch(`${API_URL}/auth/refresh`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ refresh_token: refresh })
            });

            if (res.ok) {
                const data = await res.json();
                localStorage.setItem('accessToken', data.access_token);
                return true;
            }
        } catch (err) {
            console.error('Token refresh failed:', err);
        }

        // Refresh failed, logout
        await logout();
        return false;
    };

    const value = {
        user,
        loading,
        error,
        login,
        register,
        logout,
        refreshToken,
        isAuthenticated: !!user
    };

    return (
        <AuthContext.Provider value={value}>
            {children}
        </AuthContext.Provider>
    );
}

// Login Component
export default function Login({ onClose, onSuccess }) {
    const [mode, setMode] = useState('login'); // login, register, forgot, verify2fa
    const [email, setEmail] = useState('');
    const [password, setPassword] = useState('');
    const [confirmPassword, setConfirmPassword] = useState('');
    const [firstName, setFirstName] = useState('');
    const [lastName, setLastName] = useState('');
    const [totpCode, setTotpCode] = useState('');
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState('');
    const [success, setSuccess] = useState('');

    const { login, register } = useAuth() || {};

    const handleLogin = async (e) => {
        e.preventDefault();
        setError('');
        setLoading(true);

        try {
            const result = await login(email, password, totpCode || null);
            if (result.requires2FA) {
                setMode('verify2fa');
            } else if (result.success) {
                onSuccess?.();
                onClose?.();
            }
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    const handleRegister = async (e) => {
        e.preventDefault();
        setError('');

        if (password !== confirmPassword) {
            setError('Passwords do not match');
            return;
        }

        if (password.length < 8) {
            setError('Password must be at least 8 characters');
            return;
        }

        setLoading(true);

        try {
            const result = await register(email, password, firstName, lastName);
            if (result.success) {
                setSuccess('Registration successful! Please check your email to verify your account.');
                setMode('login');
            }
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    const handleForgotPassword = async (e) => {
        e.preventDefault();
        setError('');
        setLoading(true);

        try {
            const res = await fetch(`${API_URL}/auth/forgot-password`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ email })
            });

            if (res.ok) {
                setSuccess('If an account exists, a password reset link has been sent.');
                setMode('login');
            }
        } catch (err) {
            setError('Failed to send reset email');
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="login-overlay">
            <div className="login-modal">
                <button className="login-close" onClick={onClose}>×</button>

                <div className="login-header">
                    <div className="login-logo">
                        <svg viewBox="0 0 40 40" fill="none">
                            <rect width="40" height="40" rx="10" fill="url(#loginGradient)" />
                            <path d="M12 20L18 14L24 20L30 14" stroke="white" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" />
                            <path d="M12 26L18 20L24 26L30 20" stroke="white" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" opacity="0.6" />
                            <defs>
                                <linearGradient id="loginGradient" x1="0" y1="0" x2="40" y2="40">
                                    <stop stopColor="#6366f1" />
                                    <stop offset="1" stopColor="#8b5cf6" />
                                </linearGradient>
                            </defs>
                        </svg>
                    </div>
                    <h2>
                        {mode === 'login' && 'Welcome Back'}
                        {mode === 'register' && 'Create Account'}
                        {mode === 'forgot' && 'Reset Password'}
                        {mode === 'verify2fa' && 'Two-Factor Authentication'}
                    </h2>
                    <p className="login-subtitle">
                        {mode === 'login' && 'Sign in to ACA DataHub'}
                        {mode === 'register' && 'Start analyzing your data today'}
                        {mode === 'forgot' && 'Enter your email to reset password'}
                        {mode === 'verify2fa' && 'Enter the code from your authenticator app'}
                    </p>
                </div>

                {error && <div className="login-error">{error}</div>}
                {success && <div className="login-success">{success}</div>}

                {mode === 'login' && (
                    <form onSubmit={handleLogin} className="login-form">
                        <div className="form-group">
                            <label htmlFor="email">Email</label>
                            <input
                                id="email"
                                type="email"
                                value={email}
                                onChange={(e) => setEmail(e.target.value)}
                                placeholder="you@example.com"
                                required
                                autoFocus
                            />
                        </div>
                        <div className="form-group">
                            <label htmlFor="password">Password</label>
                            <input
                                id="password"
                                type="password"
                                value={password}
                                onChange={(e) => setPassword(e.target.value)}
                                placeholder="••••••••"
                                required
                            />
                        </div>
                        <button type="submit" className="login-btn primary" disabled={loading}>
                            {loading ? 'Signing in...' : 'Sign In'}
                        </button>
                        <div className="login-links">
                            <button type="button" onClick={() => setMode('forgot')}>
                                Forgot password?
                            </button>
                            <button type="button" onClick={() => setMode('register')}>
                                Create account
                            </button>
                        </div>
                    </form>
                )}

                {mode === 'register' && (
                    <form onSubmit={handleRegister} className="login-form">
                        <div className="form-row">
                            <div className="form-group">
                                <label htmlFor="firstName">First Name</label>
                                <input
                                    id="firstName"
                                    type="text"
                                    value={firstName}
                                    onChange={(e) => setFirstName(e.target.value)}
                                    placeholder="John"
                                />
                            </div>
                            <div className="form-group">
                                <label htmlFor="lastName">Last Name</label>
                                <input
                                    id="lastName"
                                    type="text"
                                    value={lastName}
                                    onChange={(e) => setLastName(e.target.value)}
                                    placeholder="Doe"
                                />
                            </div>
                        </div>
                        <div className="form-group">
                            <label htmlFor="regEmail">Email</label>
                            <input
                                id="regEmail"
                                type="email"
                                value={email}
                                onChange={(e) => setEmail(e.target.value)}
                                placeholder="you@example.com"
                                required
                            />
                        </div>
                        <div className="form-group">
                            <label htmlFor="regPassword">Password</label>
                            <input
                                id="regPassword"
                                type="password"
                                value={password}
                                onChange={(e) => setPassword(e.target.value)}
                                placeholder="Min. 8 characters"
                                required
                            />
                        </div>
                        <div className="form-group">
                            <label htmlFor="confirmPassword">Confirm Password</label>
                            <input
                                id="confirmPassword"
                                type="password"
                                value={confirmPassword}
                                onChange={(e) => setConfirmPassword(e.target.value)}
                                placeholder="••••••••"
                                required
                            />
                        </div>
                        <button type="submit" className="login-btn primary" disabled={loading}>
                            {loading ? 'Creating account...' : 'Create Account'}
                        </button>
                        <div className="login-links">
                            <button type="button" onClick={() => setMode('login')}>
                                Already have an account? Sign in
                            </button>
                        </div>
                    </form>
                )}

                {mode === 'forgot' && (
                    <form onSubmit={handleForgotPassword} className="login-form">
                        <div className="form-group">
                            <label htmlFor="resetEmail">Email</label>
                            <input
                                id="resetEmail"
                                type="email"
                                value={email}
                                onChange={(e) => setEmail(e.target.value)}
                                placeholder="you@example.com"
                                required
                                autoFocus
                            />
                        </div>
                        <button type="submit" className="login-btn primary" disabled={loading}>
                            {loading ? 'Sending...' : 'Send Reset Link'}
                        </button>
                        <div className="login-links">
                            <button type="button" onClick={() => setMode('login')}>
                                Back to sign in
                            </button>
                        </div>
                    </form>
                )}

                {mode === 'verify2fa' && (
                    <form onSubmit={handleLogin} className="login-form">
                        <div className="form-group">
                            <label htmlFor="totpCode">Authentication Code</label>
                            <input
                                id="totpCode"
                                type="text"
                                value={totpCode}
                                onChange={(e) => setTotpCode(e.target.value)}
                                placeholder="000000"
                                maxLength={6}
                                pattern="[0-9]*"
                                required
                                autoFocus
                                className="totp-input"
                            />
                        </div>
                        <button type="submit" className="login-btn primary" disabled={loading}>
                            {loading ? 'Verifying...' : 'Verify'}
                        </button>
                        <div className="login-links">
                            <button type="button" onClick={() => { setMode('login'); setTotpCode(''); }}>
                                Back to sign in
                            </button>
                        </div>
                    </form>
                )}
            </div>
        </div>
    );
}
