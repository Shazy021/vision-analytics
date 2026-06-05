# Vision Analytics Platform
Video analytics platform: upload video → object detection (YOLOv8) → PostgreSQL storage → analytics API → Streamlit dashboard.

## Tech Stack

- **API**: FastAPI, Pydantic, SQLAlchemy
- **CV**: Ultralytics YOLOv8, OpenCV
- **БД**: PostgreSQL, Alembic (миграции)
- **Дашборд**: Streamlit, Plotly
- **Инфра**: Docker Compose, pytest

## Quick Start

### Local


```bash
# Create .env from template
cp .env.example .env

# Install dependencies
pip install -r requirements.txt

# Start PostgreSQL (or use local instance)
# Update DATABASE_URL in .env

# Run migrations
alembic upgrade head

# Start API
uvicorn app.main:app --reload

# Start dashboard (in another terminal)
streamlit run dashboard/app.py
```
## Docker
```bash
docker-compose up --build
```
- API: http://localhost:8000
- Swagger: http://localhost:8000/docs
- Dashboard: http://localhost:8501

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | /api/videos/upload | Upload video & start processing |
| GET | /api/videos | List all videos |
| GET | /api/videos/{id} | Get video details |
| GET | /api/analytics/stats | Overall statistics |
| GET | /api/analytics/class-distribution | Distribution by object class |
| GET | /api/analytics/timeline | Detections over time |



## Project Structure

```text
├── app/
│   ├── api/routes/       # API endpoints
│   ├── core/             # Config, database, settings
│   ├── db/models/        # SQLAlchemy models
│   ├── detection/        # YOLO detector
│   ├── pipeline/         # Video processing orchestrator
│   ├── schemas/          # Pydantic schemas
│   ├── video/            # Frame extraction
│   └── main.py           # FastAPI entry point
├── dashboard/            # Streamlit dashboard
├── alembic/              # Database migrations
├── tests/                # Test suite
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```