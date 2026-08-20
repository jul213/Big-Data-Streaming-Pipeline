# Enterprise Big Data Stream Ingestion & Columnar Analytics Engine

[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110%2B-green.svg)](https://fastapi.tiangolo.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 🚀 Architectural Overview
High-performance, low-latency streaming telemetry ingestion and columnar analytics engine designed for modern distributed architectures (2026-2030 standards). The system implements a **Micro-Batch Window Pattern** to process high-velocity data streams in memory utilizing Pandas vectorization, ensuring optimal analytical throughput without external heavy infrastructure dependencies.

---

## 🛠️ Core Features
- **Asynchronous Ingestion API**: Built on FastAPI to handle high-concurrency event streams with strict Pydantic v2 data contracts.
- **Micro-Batch Windowing**: Decoupled buffer system that aggregates incoming data streams and triggers columnar processing on-demand.
- **In-Memory Columnar Analytics**: Leverages Pandas for rapid statistical aggregation (cardinality, frequency distribution, payload averages, and latency tracking).
- **Production-Ready Observability**: Structured logging and automated health-check endpoints for containerized orchestration (Docker/Kubernetes).

---

## 📂 Project Structure
```text
bigdata-stream-engine/
├── src/
│   ├── __init__.py
│   ├── pipeline.py      # Core stream processor & columnar analytics logic
│   └── api.py           # FastAPI asynchronous ingestion endpoints
├── data/                # Local data persistence / buffer storage
├── requirements.txt     # Strict production dependencies
└── README.md            # Technical documentation
