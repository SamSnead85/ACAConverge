import { useState, useEffect } from 'react';

/**
 * Visual Query Builder - Create SQL queries with a point-and-click UI
 */
export default function QueryBuilder({ schema, onQueryBuilt, initialQuery = '' }) {
    const [conditions, setConditions] = useState([]);
    const [orderBy, setOrderBy] = useState({ column: '', direction: 'ASC' });
    const [limit, setLimit] = useState(1000);
    const [selectedColumns, setSelectedColumns] = useState([]);

    const columns = schema?.map(s => s.name) || [];
    const columnTypes = schema?.reduce((acc, s) => ({ ...acc, [s.name]: s.sql_type }), {}) || {};

    const operators = {
        text: ['equals', 'not equals', 'contains', 'starts with', 'ends with', 'is null', 'is not null'],
        number: ['=', '!=', '>', '<', '>=', '<=', 'between', 'is null', 'is not null'],
        date: ['=', '!=', '>', '<', '>=', '<=', 'between', 'is null', 'is not null']
    };

    const getOperatorsForColumn = (columnName) => {
        const type = columnTypes[columnName];
        if (['INTEGER', 'REAL'].includes(type)) return operators.number;
        if (type === 'DATE' || type === 'DATETIME') return operators.date;
        return operators.text;
    };

    const addCondition = () => {
        setConditions([...conditions, {
            id: Date.now(),
            column: columns[0] || '',
            operator: 'equals',
            value: '',
            value2: '', // For between
            logic: 'AND'
        }]);
    };

    const updateCondition = (id, field, value) => {
        setConditions(conditions.map(c =>
            c.id === id ? { ...c, [field]: value } : c
        ));
    };

    const removeCondition = (id) => {
        setConditions(conditions.filter(c => c.id !== id));
    };

    const toggleColumn = (col) => {
        setSelectedColumns(prev =>
            prev.includes(col)
                ? prev.filter(c => c !== col)
                : [...prev, col]
        );
    };

    const buildQuery = () => {
        // SELECT clause
        const selectCols = selectedColumns.length > 0
            ? selectedColumns.map(c => `"${c}"`).join(', ')
            : '*';

        let query = `SELECT ${selectCols} FROM converted_data`;

        // WHERE clause
        if (conditions.length > 0) {
            const whereClauses = conditions.map((c, i) => {
                let clause = '';
                if (i > 0) clause = ` ${c.logic} `;

                const col = `"${c.column}"`;

                switch (c.operator) {
                    case 'equals':
                    case '=':
                        clause += `${col} = '${c.value}'`;
                        break;
                    case 'not equals':
                    case '!=':
                        clause += `${col} != '${c.value}'`;
                        break;
                    case 'contains':
                        clause += `${col} LIKE '%${c.value}%'`;
                        break;
                    case 'starts with':
                        clause += `${col} LIKE '${c.value}%'`;
                        break;
                    case 'ends with':
                        clause += `${col} LIKE '%${c.value}'`;
                        break;
                    case '>':
                    case '<':
                    case '>=':
                    case '<=':
                        clause += `${col} ${c.operator} ${c.value}`;
                        break;
                    case 'between':
                        clause += `${col} BETWEEN ${c.value} AND ${c.value2}`;
                        break;
                    case 'is null':
                        clause += `${col} IS NULL`;
                        break;
                    case 'is not null':
                        clause += `${col} IS NOT NULL`;
                        break;
                    default:
                        clause += `${col} = '${c.value}'`;
                }

                return clause;
            });

            query += ` WHERE ${whereClauses.join('')}`;
        }

        // ORDER BY
        if (orderBy.column) {
            query += ` ORDER BY "${orderBy.column}" ${orderBy.direction}`;
        }

        // LIMIT
        if (limit) {
            query += ` LIMIT ${limit}`;
        }

        return query;
    };

    const handleBuildQuery = () => {
        const query = buildQuery();
        onQueryBuilt?.(query);
    };

    return (
        <div className="query-builder">
            <h3>🔧 Visual Query Builder</h3>

            {/* Column Selection */}
            <section className="qb-section">
                <h4>Select Columns</h4>
                <div className="qb-columns">
                    <label className="qb-column-item">
                        <input
                            type="checkbox"
                            checked={selectedColumns.length === 0}
                            onChange={() => setSelectedColumns([])}
                        />
                        All Columns
                    </label>
                    {columns.map(col => (
                        <label key={col} className="qb-column-item">
                            <input
                                type="checkbox"
                                checked={selectedColumns.includes(col)}
                                onChange={() => toggleColumn(col)}
                            />
                            {col}
                            <span className="qb-col-type">{columnTypes[col]}</span>
                        </label>
                    ))}
                </div>
            </section>

            {/* Conditions */}
            <section className="qb-section">
                <h4>Filter Conditions</h4>
                {conditions.length === 0 ? (
                    <p className="qb-empty">No filters added. Click "Add Condition" to filter data.</p>
                ) : (
                    <div className="qb-conditions">
                        {conditions.map((condition, index) => (
                            <div key={condition.id} className="qb-condition">
                                {index > 0 && (
                                    <select
                                        className="qb-logic"
                                        value={condition.logic}
                                        onChange={(e) => updateCondition(condition.id, 'logic', e.target.value)}
                                    >
                                        <option value="AND">AND</option>
                                        <option value="OR">OR</option>
                                    </select>
                                )}

                                <select
                                    className="qb-select"
                                    value={condition.column}
                                    onChange={(e) => updateCondition(condition.id, 'column', e.target.value)}
                                >
                                    {columns.map(col => (
                                        <option key={col} value={col}>{col}</option>
                                    ))}
                                </select>

                                <select
                                    className="qb-select"
                                    value={condition.operator}
                                    onChange={(e) => updateCondition(condition.id, 'operator', e.target.value)}
                                >
                                    {getOperatorsForColumn(condition.column).map(op => (
                                        <option key={op} value={op}>{op}</option>
                                    ))}
                                </select>

                                {!['is null', 'is not null'].includes(condition.operator) && (
                                    <input
                                        type="text"
                                        className="qb-input"
                                        value={condition.value}
                                        onChange={(e) => updateCondition(condition.id, 'value', e.target.value)}
                                        placeholder="Value"
                                    />
                                )}

                                {condition.operator === 'between' && (
                                    <>
                                        <span className="qb-and">and</span>
                                        <input
                                            type="text"
                                            className="qb-input"
                                            value={condition.value2}
                                            onChange={(e) => updateCondition(condition.id, 'value2', e.target.value)}
                                            placeholder="Value 2"
                                        />
                                    </>
                                )}

                                <button
                                    className="qb-remove"
                                    onClick={() => removeCondition(condition.id)}
                                    title="Remove condition"
                                >
                                    ✕
                                </button>
                            </div>
                        ))}
                    </div>
                )}
                <button className="btn btn-sm" onClick={addCondition}>
                    ➕ Add Condition
                </button>
            </section>

            {/* Order By */}
            <section className="qb-section">
                <h4>Sort Results</h4>
                <div className="qb-orderby">
                    <select
                        className="qb-select"
                        value={orderBy.column}
                        onChange={(e) => setOrderBy({ ...orderBy, column: e.target.value })}
                    >
                        <option value="">No sorting</option>
                        {columns.map(col => (
                            <option key={col} value={col}>{col}</option>
                        ))}
                    </select>
                    <select
                        className="qb-select"
                        value={orderBy.direction}
                        onChange={(e) => setOrderBy({ ...orderBy, direction: e.target.value })}
                        disabled={!orderBy.column}
                    >
                        <option value="ASC">Ascending</option>
                        <option value="DESC">Descending</option>
                    </select>
                </div>
            </section>

            {/* Limit */}
            <section className="qb-section">
                <h4>Limit Results</h4>
                <input
                    type="number"
                    className="qb-input"
                    value={limit}
                    onChange={(e) => setLimit(parseInt(e.target.value) || 1000)}
                    min={1}
                    max={100000}
                />
            </section>

            {/* Preview & Build */}
            <section className="qb-section">
                <h4>Generated Query</h4>
                <pre className="qb-preview">{buildQuery()}</pre>
                <button className="btn btn-primary" onClick={handleBuildQuery}>
                    ▶️ Run Query
                </button>
            </section>
        </div>
    );
}
