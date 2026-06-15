# Windows Dev Quickstart

Target: Windows PowerShell, Python 3.11 x64, Node 20 LTS, safe/dev mode.

## Prerequisites

- Python 3.11 x64 available through `py -3.11`
- Node 20 LTS (`>=20.17.0 <21`) with npm 10.x
- Git
- Docker Desktop for local Timescale/PostgreSQL, Redis, and MinIO

## External Services

Start local services. The Timescale container needs more shared memory than Docker's default for this repo's validation and startup path.

```powershell
docker run -d --name trading-timescaledb-windows --shm-size=512m -e POSTGRES_DB=trading -e POSTGRES_USER=trading -e POSTGRES_PASSWORD=local-timescale-password -p 15433:5432 timescale/timescaledb:latest-pg16
docker run -d --name trading-redis -p 16379:6379 redis:7-alpine redis-server --appendonly yes --requirepass local-redis-password
docker run -d --name trading-minio -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=local-minio-password -p 19000:9000 -p 19001:9001 minio/minio:RELEASE.2025-04-22T22-12-26Z server /data --console-address :9001
```

If a container already exists, start it instead:

```powershell
docker start trading-timescaledb-windows trading-redis trading-minio
```

Set `.env` to point at those host ports, or let `start_system.py safe` create `.env` from `.env.example` and then adjust the service URLs. Keep live broker and paid feed toggles disabled for safe/dev mode.

## Install

From the repo root:

```powershell
py -3.11 -m venv .venv
.venv\Scripts\python.exe -m pip install --upgrade pip wheel setuptools
.venv\Scripts\pip.exe install -r requirements.txt
npm ci
npm run check:ui
```

Pre-cache the sentence-transformers model so first boot does not block on the download:

```powershell
.venv\Scripts\python.exe -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')"
```

## Configure

First safe-mode boot creates `data/`, `logs/`, and `.env` when missing, then appends a generated `DATA_SOURCE_MASTER_KEY`.

For the local service ports above, `.env` should include the corresponding PostgreSQL/Redis/MinIO settings, for example:

```text
TS_PG_DSN=host=127.0.0.1 port=15433 user=trading dbname=trading password=local-timescale-password
REDIS_URL=redis://:local-redis-password@127.0.0.1:16379/0
MINIO_ENDPOINT=http://127.0.0.1:19000
ENGINE_MODE=safe
EXECUTION_MODE=safe
KILL_SWITCH_GLOBAL=1
```

Do not add IBKR, Alpaca, CCXT, Polygon, Tradier, or other live/premium credentials unless intentionally testing those providers.

## Validate

```powershell
.venv\Scripts\python.exe tools\validate_repo.py
npm run check:ui
```

## Start

Terminal A:

```powershell
.venv\Scripts\python.exe start_system.py safe
```

Terminal B:

```powershell
node boot/operator_server.js
```

Health checks:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/health
Invoke-RestMethod http://127.0.0.1:4001/api/operator/ping
```

Dashboard:

```text
http://127.0.0.1:8000/ui/dashboard.html
```

Runtime pid files:

```powershell
Get-Content logs\runtime.pid
Get-Content logs\ingestion.pid
```
