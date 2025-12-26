// Query Templates and Presets
export const QUERY_TEMPLATES = [
    {
        id: 'all-records',
        name: 'Show All Records',
        category: 'Basic',
        query: 'Show all records',
        description: 'Display all records in the table',
        icon: '📋'
    },
    {
        id: 'count-total',
        name: 'Count Total',
        category: 'Basic',
        query: 'Count total records',
        description: 'Get the total number of records',
        icon: '🔢'
    },
    {
        id: 'top-10',
        name: 'Top 10',
        category: 'Basic',
        query: 'Show top 10 records',
        description: 'Display the first 10 records',
        icon: '🔝'
    },
    {
        id: 'unique-values',
        name: 'Unique Values',
        category: 'Analysis',
        query: 'Show unique values in {column}',
        description: 'List distinct values for a column',
        icon: '🎯',
        requiresColumn: true
    },
    {
        id: 'group-by-count',
        name: 'Group & Count',
        category: 'Analysis',
        query: 'Count records grouped by {column}',
        description: 'Count records for each unique value',
        icon: '📊',
        requiresColumn: true
    },
    {
        id: 'sum-by-group',
        name: 'Sum by Group',
        category: 'Analysis',
        query: 'Sum of {column1} grouped by {column2}',
        description: 'Calculate sum for each group',
        icon: '➕',
        requiresColumn: true
    },
    {
        id: 'average',
        name: 'Average Value',
        category: 'Statistics',
        query: 'Average of {column}',
        description: 'Calculate the average value',
        icon: '📈',
        requiresColumn: true
    },
    {
        id: 'min-max',
        name: 'Min & Max',
        category: 'Statistics',
        query: 'Show minimum and maximum of {column}',
        description: 'Find the range of values',
        icon: '↕️',
        requiresColumn: true
    },
    {
        id: 'filter-greater',
        name: 'Filter Greater Than',
        category: 'Filters',
        query: 'Show records where {column} > {value}',
        description: 'Filter by numeric comparison',
        icon: '⬆️',
        requiresInput: true
    },
    {
        id: 'filter-contains',
        name: 'Filter Contains',
        category: 'Filters',
        query: 'Show records where {column} contains "{value}"',
        description: 'Filter by text search',
        icon: '🔍',
        requiresInput: true
    },
    {
        id: 'filter-null',
        name: 'Find Nulls',
        category: 'Filters',
        query: 'Show records where {column} is null',
        description: 'Find records with missing values',
        icon: '❓',
        requiresColumn: true
    },
    {
        id: 'order-asc',
        name: 'Order Ascending',
        category: 'Sorting',
        query: 'Show all records ordered by {column} ascending',
        description: 'Sort from lowest to highest',
        icon: '⬆️',
        requiresColumn: true
    },
    {
        id: 'order-desc',
        name: 'Order Descending',
        category: 'Sorting',
        query: 'Show all records ordered by {column} descending',
        description: 'Sort from highest to lowest',
        icon: '⬇️',
        requiresColumn: true
    },
    {
        id: 'date-range',
        name: 'Date Range',
        category: 'Date',
        query: 'Show records where {column} is between {start} and {end}',
        description: 'Filter by date range',
        icon: '📅',
        requiresInput: true
    },
    {
        id: 'recent-records',
        name: 'Recent Records',
        category: 'Date',
        query: 'Show records from the last 30 days',
        description: 'Filter by recent dates',
        icon: '🕐'
    }
];

// Group templates by category
export function getTemplatesByCategory() {
    const grouped = {};
    QUERY_TEMPLATES.forEach(template => {
        if (!grouped[template.category]) {
            grouped[template.category] = [];
        }
        grouped[template.category].push(template);
    });
    return grouped;
}

// Get template by ID
export function getTemplateById(id) {
    return QUERY_TEMPLATES.find(t => t.id === id);
}

// Apply variables to template
export function applyTemplateVariables(template, variables) {
    let query = template.query;
    Object.entries(variables).forEach(([key, value]) => {
        query = query.replace(`{${key}}`, value);
    });
    return query;
}

export default QUERY_TEMPLATES;
