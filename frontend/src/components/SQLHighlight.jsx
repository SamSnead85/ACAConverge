import { useState, useMemo } from 'react';

// Simple SQL syntax highlighter
export function highlightSQL(sql) {
    if (!sql) return '';

    const keywords = [
        'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'ORDER BY', 'GROUP BY', 'HAVING',
        'LIMIT', 'OFFSET', 'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN',
        'ON', 'AS', 'IN', 'NOT', 'NULL', 'IS', 'LIKE', 'BETWEEN', 'DISTINCT',
        'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'ASC', 'DESC', 'CASE', 'WHEN',
        'THEN', 'ELSE', 'END', 'WITH', 'UNION', 'ALL', 'EXISTS', 'COALESCE'
    ];

    const functions = [
        'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'ROUND', 'UPPER', 'LOWER',
        'LENGTH', 'SUBSTR', 'TRIM', 'COALESCE', 'IFNULL', 'NULLIF',
        'DATE', 'TIME', 'DATETIME', 'STRFTIME', 'ABS', 'CAST'
    ];

    let highlighted = sql;

    // Escape HTML
    highlighted = highlighted
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');

    // Highlight strings (single quotes)
    highlighted = highlighted.replace(
        /'([^']*)'/g,
        '<span class="sql-string">\'$1\'</span>'
    );

    // Highlight numbers
    highlighted = highlighted.replace(
        /\b(\d+(?:\.\d+)?)\b/g,
        '<span class="sql-number">$1</span>'
    );

    // Highlight keywords (case insensitive)
    keywords.forEach(keyword => {
        const regex = new RegExp(`\\b(${keyword})\\b`, 'gi');
        highlighted = highlighted.replace(
            regex,
            '<span class="sql-keyword">$1</span>'
        );
    });

    // Highlight table/column names in quotes
    highlighted = highlighted.replace(
        /"([^"]*)"/g,
        '<span class="sql-identifier">"$1"</span>'
    );

    // Highlight operators
    highlighted = highlighted.replace(
        /([=<>!]+|(?:&lt;|&gt;)=?)/g,
        '<span class="sql-operator">$1</span>'
    );

    return highlighted;
}

// SQL Highlighter Component
export function SQLHighlight({ sql, className = '' }) {
    const highlighted = useMemo(() => highlightSQL(sql), [sql]);

    return (
        <pre
            className={`sql-highlight ${className}`}
            dangerouslySetInnerHTML={{ __html: highlighted }}
        />
    );
}

// SQL Editor with line numbers
export function SQLEditor({ value, onChange, readOnly = false, placeholder = '' }) {
    const lines = value.split('\n');

    return (
        <div className="sql-editor">
            <div className="sql-line-numbers">
                {lines.map((_, i) => (
                    <div key={i} className="sql-line-number">{i + 1}</div>
                ))}
            </div>
            <textarea
                className="sql-editor-input"
                value={value}
                onChange={(e) => onChange(e.target.value)}
                readOnly={readOnly}
                placeholder={placeholder}
                spellCheck={false}
            />
            <div
                className="sql-editor-highlight"
                dangerouslySetInnerHTML={{ __html: highlightSQL(value) + '\n' }}
            />
        </div>
    );
}

export default SQLHighlight;
