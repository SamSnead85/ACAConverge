# ACA DataHub

> **AI-Powered Data Analytics Platform** | Convert • Analyze • Engage

Transform any data file into actionable insights with natural language queries powered by Google Gemini AI.

![ACA DataHub](https://img.shields.io/badge/Powered%20by-Gemini%20AI-4285F4?style=for-the-badge&logo=google&logoColor=white)
![Version](https://img.shields.io/badge/Version-2.0.0-6366f1?style=for-the-badge)
![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)

---

## ✨ Features

### 📤 Multi-Format Import
- **Alteryx (.yxdb)** - Native Alteryx database files
- **Excel (.xlsx, .xls)** - Microsoft Excel workbooks
- **CSV** - Comma-separated values
- **JSON** - JSON arrays or newline-delimited
- Up to **50GB** file support with streaming uploads

### 🤖 AI-Powered Analysis
- **Natural Language Queries** - Ask questions in plain English
- **Smart Suggestions** - AI recommends relevant analyses based on your data
- **Visual Query Builder** - Point-and-click SQL generation
- **Auto-Insights** - Automatic pattern and trend detection

### � Population Management
- Create segments from query results
- Combine populations (Union, Intersect, Exclude)
- Track population sizes over time
- Export segments for external use

### � Reporting
- **Summary Reports** - Column statistics and aggregates
- **Detailed Reports** - Full record exports
- **Comparison Reports** - Compare multiple segments
- Export as CSV, JSON, or HTML

### 📨 Messaging & Outreach
- Create email/SMS templates with variable substitution
- Preview messages with sample data
- Dry-run testing before sending
- Track send history

### ⚙️ Database Options
- **SQLite** - File-based, zero configuration
- **PostgreSQL** - Production-grade, scalable

---

## 🚀 Quick Start

### Prerequisites
- Node.js 18+
- Python 3.9+
- Google Gemini API key

### Installation

```bash
# Clone the repository
git clone https://github.com/SamSnead85/ACAConverge.git
cd ACAConverge

# Backend setup
cd backend
pip install -r requirements.txt
cp .env.example .env
# Add your GEMINI_API_KEY to .env

# Start backend
uvicorn main:app --reload --port 8000

# Frontend setup (new terminal)
cd frontend
npm install
npm run dev
```

### Environment Variables

**Backend (.env)**
```env
GEMINI_API_KEY=your_gemini_api_key
FRONTEND_URL=http://localhost:5173
```

**Frontend (.env)**
```env
VITE_API_URL=http://localhost:8000/api
```

---

## 📱 Navigation

| Tab | Description |
|-----|-------------|
| **Import** | Upload data files (YXDB, CSV, Excel, JSON) |
| **Insights** | Dashboard with AI-powered analytics |
| **AI Query** | Natural language and visual query builder |
| **Segments** | Population management and segmentation |
| **Reports** | Generate and export reports |
| **Outreach** | Message templates and sending |
| **Settings** | Database configuration |

---

## 🔌 API Reference

### Conversion
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/upload` | Upload and convert file |
| GET | `/api/conversion/status/:id` | Check conversion progress |
| GET | `/api/schema/:id` | Get database schema |
| GET | `/api/download/:id` | Download SQLite database |

### Query
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/query` | Natural language query |
| POST | `/api/query/sql` | Direct SQL query |
| GET | `/api/query/history` | Query history |

### Populations
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/populations/:job_id` | Create population |
| GET | `/api/populations/:job_id` | List populations |
| POST | `/api/populations/combine` | Combine populations |
| DELETE | `/api/population/:id` | Delete population |

### Reports & Messaging
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/report/summary/:pop_id` | Generate summary |
| POST | `/api/templates` | Create message template |
| POST | `/api/messaging/send` | Send messages |

---

## 🛠️ Tech Stack

### Frontend
- **React 18** + Vite 7
- Custom CSS design system
- Glassmorphism UI components
- Inter typography

### Backend
- **FastAPI** (Python)
- **Google Gemini** for NLP
- SQLite / PostgreSQL
- Streaming file processing

---

## 📦 Project Structure

```
ACAConverge/
├── backend/
│   ├── main.py              # FastAPI entry point
│   ├── routes/              # API endpoints
│   │   ├── conversion.py    # File upload/conversion
│   │   ├── query.py         # NLP and SQL queries
│   │   ├── population.py    # Segment management
│   │   ├── reporting.py     # Report generation
│   │   ├── messaging.py     # Message templates/sending
│   │   ├── database.py      # DB configuration
│   │   └── scheduler.py     # Scheduled jobs
│   └── services/            # Business logic
│       ├── nlp_query.py     # Gemini integration
│       ├── sql_converter.py # Data conversion
│       ├── file_parser.py   # Multi-format parsing
│       └── database.py      # DB abstraction
├── frontend/
│   ├── src/
│   │   ├── App.jsx          # Main application
│   │   ├── components/      # React components
│   │   └── styles/          # CSS modules
│   └── index.html
├── docs/
│   └── postman_collection.json
└── README.md
```

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing`)
5. Open a Pull Request

---

## 📄 License

MIT License - see [LICENSE](LICENSE) for details.

---

<p align="center">
  <strong>ACA DataHub</strong> | Powered by Google Gemini AI
</p>
