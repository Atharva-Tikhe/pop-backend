# Pipeline Observability Platform Backend - Project Context & Mandates

## Project Overview
This repository is the FastAPI-based backend for POP. It is designed to manage, trigger and monitor Nextflow pipelines.


This project is a FastAPI-based orchestration layer designed to manage, trigger, and monitor Nextflow pipelines. It provides an interface for user uploads, manifest generation, and real-time execution tracking.

## Core Stack
- **Web Framework:** FastAPI (Asynchronous Python)
- **Task Queue:** Celery with Redis broker
- **Persistence:** PostgreSQL (SQLAlchemy ORM)
- **Real-time Updates:** Redis Pub/Sub + WebSockets
- **Validation:** Pydantic models

## Architectural Mandates
- **Task Execution:** All long-running processes (e.g., calling `nextflow run`) MUST be handled by Celery workers in `celery_app.py`.
- **State Management:** Use Redis hashes (`pipeline_state:<id>`) for transient execution data and PostgreSQL for persistent audit trails.
- **Monitoring:** Real-time feedback is driven by Nextflow Weblog callbacks. Any changes to the monitoring logic must align with the normalization patterns in `weblogs/`.
- **Type Safety:** Maintain strict Pydantic models in `models.py` for all API request/response cycles.
- **Service Layer:** Business logic should reside in `service.py` to keep `main.py` (routes) and `celery_app.py` (tasks) lean.

## Key Entry Points
- `main.py`: FastAPI application, route definitions, and WebSocket handlers.
- `celery_app.py`: Celery worker configuration and task definitions for pipeline execution.
- `db/model.py`: Database schema definitions.
- `snapshot_weblogs.py`: Logic for ingesting and processing Nextflow execution events.

## Data Flow: User to Pipeline
1. **Upload (`/upload`):** Files are validated by `UploadValidator` and stored in `uploads/`.
2. **Submission (`/submit`):** Manifest is generated; database record is created; Celery task is dispatched.
3. **Execution:** Celery worker runs `nextflow run`.
4. **Monitoring:** Nextflow pushes JSON events to `/nextflow/weblog`, which are processed and broadcasted via WebSockets.
