// Loading Components

// Spinner
export function Spinner({ size = 'medium', className = '' }) {
    return (
        <div className={`spinner spinner-${size} ${className}`}></div>
    );
}

// Skeleton Loader
export function Skeleton({ width = '100%', height = '1rem', rounded = false, className = '' }) {
    return (
        <div
            className={`skeleton ${rounded ? 'skeleton-rounded' : ''} ${className}`}
            style={{ width, height }}
        ></div>
    );
}

// Skeleton Text Lines
export function SkeletonText({ lines = 3, className = '' }) {
    return (
        <div className={`skeleton-text ${className}`}>
            {Array.from({ length: lines }).map((_, i) => (
                <Skeleton
                    key={i}
                    width={i === lines - 1 ? '60%' : '100%'}
                    height="0.875rem"
                />
            ))}
        </div>
    );
}

// Skeleton Card
export function SkeletonCard({ className = '' }) {
    return (
        <div className={`card ${className}`}>
            <Skeleton height="1.5rem" width="60%" />
            <div style={{ marginTop: '1rem' }}>
                <SkeletonText lines={3} />
            </div>
        </div>
    );
}

// Skeleton Table
export function SkeletonTable({ rows = 5, columns = 4, className = '' }) {
    return (
        <div className={`skeleton-table ${className}`}>
            <div className="skeleton-table-header">
                {Array.from({ length: columns }).map((_, i) => (
                    <Skeleton key={i} height="1rem" />
                ))}
            </div>
            {Array.from({ length: rows }).map((_, rowIndex) => (
                <div key={rowIndex} className="skeleton-table-row">
                    {Array.from({ length: columns }).map((_, colIndex) => (
                        <Skeleton
                            key={colIndex}
                            height="0.875rem"
                            width={`${60 + Math.random() * 30}%`}
                        />
                    ))}
                </div>
            ))}
        </div>
    );
}

// Full Page Loading
export function PageLoader({ message = 'Loading...' }) {
    return (
        <div className="page-loader">
            <Spinner size="large" />
            <p className="page-loader-message">{message}</p>
        </div>
    );
}

// Button Loading State
export function LoadingButton({ loading, children, ...props }) {
    return (
        <button {...props} disabled={loading || props.disabled}>
            {loading ? (
                <>
                    <Spinner size="small" />
                    <span>Loading...</span>
                </>
            ) : children}
        </button>
    );
}

// Progress indicator for long operations
export function ProgressIndicator({ value, max = 100, showLabel = true, size = 'medium' }) {
    const percentage = Math.min((value / max) * 100, 100);

    return (
        <div className={`progress-indicator progress-${size}`}>
            {showLabel && (
                <div className="progress-indicator-label">
                    <span>{Math.round(percentage)}%</span>
                </div>
            )}
            <div className="progress-indicator-track">
                <div
                    className="progress-indicator-fill"
                    style={{ width: `${percentage}%` }}
                ></div>
            </div>
        </div>
    );
}

export default Spinner;
