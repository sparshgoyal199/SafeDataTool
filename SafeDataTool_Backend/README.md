# SafeData Tool - Privacy-Utility Pipeline

A comprehensive framework for evaluating and enhancing data privacy while preserving analytical utility, aligned with the Digital Personal Data Protection (DPDP) Act, 2023.

## Features

- **Risk Assessment**: Simulates linkage attacks to quantify re-identification risk
- **Privacy Enhancement**: Implements k-anonymity and Differential Privacy
- **Utility Measurement**: Compares protected vs original datasets
- **Automated Reporting**: Generates HTML/PDF Privacy-Utility reports
- **Background Jobs**: Optional Celery-based async processing for long-running tasks
- **Web Dashboard**: Modern UI for dataset management and pipeline execution

## Setup

### 1. Environment Configuration

Create a `.env` file in `SafeDataTool_Backend/`:

```env
# Database
DATABASE_URL=sqlite:///./safedata.db

# JWT
SECRET_KEY=your-secret-key-min-32-chars
JWT_SECRET_KEY=your-secret-key-min-32-chars
JWT_ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=1440

# CORS
ALLOW_ORIGINS=http://localhost:8000,http://127.0.0.1:8000,http://localhost:5500

# Background Jobs (optional)
USE_BACKGROUND_JOBS=false
CELERY_BROKER_URL=redis://localhost:6379/0
CELERY_RESULT_BACKEND=redis://localhost:6379/0
```

### 2. Install Dependencies

```bash
cd SafeDataTool_Backend
pip install -r requirements.txt
```

**Note**: For PDF generation on Windows, you may need to install WeasyPrint dependencies:
- Install GTK+ runtime: https://github.com/tschoonj/GTK-for-Windows-Runtime-Environment-Installer

### 3. Initialize Database

The database will be created automatically on first run. To manually initialize:

```python
from app.db.session import init_db
init_db()
```

### 4. Run Backend

```bash
cd SafeDataTool_Backend
python main.py
```

Or with uvicorn:

```bash
uvicorn main:app --reload --host 127.0.0.1 --port 8000
```

### 5. Run Frontend

Serve the frontend using any HTTP server:

```bash
cd SafeDataTool_Frontend
python -m http.server 5500
```

Or use Live Server in VS Code.

### 6. Background Jobs (Optional)

If `USE_BACKGROUND_JOBS=true`:

1. Start Redis:
   ```bash
   redis-server
   ```

2. Start Celery worker:
   ```bash
   cd SafeDataTool_Backend
   celery -A app.workers.celery_app worker --loglevel=info
   ```

## Usage

1. **Sign Up/Login**: Create an account or login at `http://localhost:5500/html/login.html`

2. **Upload Dataset**: Upload a CSV file with your microdata

3. **Create Configuration**: Define quasi-identifiers and privacy technique parameters

4. **Execute Pipeline**: Run the pipeline to:
   - Assess re-identification risk
   - Apply privacy enhancement
   - Measure utility preservation
   - Generate comprehensive report

5. **Download Results**: Download protected datasets and reports

## Testing

Run unit tests:

```bash
cd SafeDataTool_Backend
pytest tests/
```

## Project Structure

```
SafeDataTool_Backend/
├── app/
│   ├── api/routes/          # API endpoints
│   ├── config/               # Configuration
│   ├── core/                 # Security, auth
│   ├── db/                   # Database models
│   ├── pipeline/
│   │   ├── risk/             # Risk assessment
│   │   ├── privacy/          # Privacy enhancement
│   │   ├── utility/           # Utility evaluation
│   │   └── reporting/         # Report generation
│   ├── schemas/              # Pydantic schemas
│   ├── services/             # Business logic
│   └── workers/                # Background jobs
├── data/                      # Uploaded datasets
├── reports/                   # Generated reports
├── samples/                   # Sample data
└── tests/                     # Unit tests
```

## API Endpoints

- `POST /auth/signup` - User registration
- `POST /auth/signin` - User login
- `POST /datasets` - Upload dataset
- `GET /datasets` - List datasets
- `POST /pipeline/configs` - Create pipeline configuration
- `POST /pipeline/runs` - Execute pipeline
- `GET /pipeline/runs/{id}` - Get run details
- `GET /pipeline/runs/{id}/report` - Download report
- `GET /pipeline/runs/{id}/protected` - Download protected dataset

## License

Developed for National Statistical Office (NSO) under DPDP Act, 2023 compliance.

