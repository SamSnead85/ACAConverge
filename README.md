# YXDB to SQL Converter

A web application to convert Alteryx .yxdb database files into SQLite databases with natural language query capabilities powered by AI.

![YXDB Converter](https://via.placeholder.com/800x400?text=YXDB+Converter)

## Features

- 📤 **File Upload**: Drag-and-drop interface for .yxdb files (supports 10GB+)
- 🔄 **Streaming Conversion**: Memory-efficient chunked processing
- 📋 **Schema Extraction**: Automatic field detection and type mapping
- 💬 **NLP Queries**: Ask questions in plain English - AI converts to SQL
- 📊 **Results Display**: Interactive table with pagination
- 📥 **Export Options**: Download results as CSV, JSON, or SQLite database
- 📜 **Query History**: Track and re-run previous queries

## Tech Stack

- **Backend**: Python, FastAPI, SQLite
- **Frontend**: React, Vite
- **AI**: Google Gemini 2.0 Flash
- **File Processing**: yxdb library

## Quick Start

### Prerequisites

- Python 3.9+
- Node.js 18+
- Gemini API Key (for NLP features)

### Backend Setup

```bash
cd backend

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env and add your GEMINI_API_KEY

# Start server
uvicorn main:app --reload --port 8000
```

### Frontend Setup

```bash
cd frontend

# Install dependencies
npm install

# Configure environment
cp .env.example .env
# Edit .env if backend URL differs

# Start development server
npm run dev
```

Visit `http://localhost:5173` to use the application.

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/upload` | Upload .yxdb file |
| `POST` | `/api/upload/demo` | Start demo with mock data |
| `GET` | `/api/conversion/status/{id}` | Get conversion progress |
| `GET` | `/api/schema/{id}` | Get extracted schema |
| `GET` | `/api/download/{id}` | Download SQLite database |
| `POST` | `/api/query` | Submit NLP query |
| `POST` | `/api/query/sql` | Execute direct SQL |
| `GET` | `/api/query/history/{id}` | Get query history |

## Deployment

### Frontend (Netlify)

1. Connect your GitHub repository to Netlify
2. Set build settings:
   - Base directory: `frontend`
   - Build command: `npm run build`
   - Publish directory: `frontend/dist`
3. Add environment variable: `VITE_API_URL=https://your-backend.railway.app/api`

### Backend (Railway/Render)

1. Create new project from GitHub
2. Set root directory to `backend`
3. Add environment variables:
   - `GEMINI_API_KEY=your_api_key`
   - `FRONTEND_URL=https://your-app.netlify.app`
4. Deploy

## Usage

### With Real .yxdb Files

1. Click "Upload" or drag-and-drop your .yxdb file
2. Wait for conversion to complete
3. View the extracted schema
4. Use natural language to query your data

### Demo Mode

1. Click "Try with Demo Data"
2. 50,000 sample records will be generated
3. Explore the query interface

### Example Queries

- "Show all records"
- "Count total records"
- "Show me records where sales > 1000"
- "Get the top 10 records by sales"
- "Average sales by region"

## Project Structure

```
├── backend/
│   ├── main.py              # FastAPI application
│   ├── requirements.txt     # Python dependencies
│   ├── routes/
│   │   ├── conversion.py    # File upload & conversion
│   │   └── query.py         # NLP queries
│   └── services/
│       ├── yxdb_parser.py   # YXDB file reading
│       ├── sql_converter.py # SQLite conversion
│       └── nlp_query.py     # Gemini AI integration
├── frontend/
│   ├── src/
│   │   ├── App.jsx          # Main application
│   │   ├── components/      # React components
│   │   └── utils/           # API utilities
│   └── package.json
├── netlify.toml             # Netlify configuration
└── README.md
```

## Environment Variables

### Backend

| Variable | Description | Required |
|----------|-------------|----------|
| `GEMINI_API_KEY` | Google Gemini API key | Yes (for NLP) |
| `FRONTEND_URL` | Frontend URL for CORS | No |

### Frontend

| Variable | Description | Default |
|----------|-------------|---------|
| `VITE_API_URL` | Backend API URL | `http://localhost:8000/api` |

## License

MIT

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Open a Pull Request
