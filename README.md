# Steam Data Pipeline

Containerized ETL pipeline project built with Python, Pandas, MySQL, and Docker Compose.

---

## Project Overview

This project:

- Extracts game data from the SteamSpy API
- Transforms and enriches the data using Pandas
- Loads the data into a MySQL database
- Runs inside Docker containers using Docker Compose

---

## Technologies Used

- Python
- Pandas
- Requests
- MySQL
- MySQL Connector
- Docker
- Docker Compose
- Git & GitHub

---

## Pipeline Flow

SteamSpy API → Python ETL → Pandas Transformation → MySQL

---

## Features

- API data extraction
- JSON parsing
- Pandas transformations
- Owners range parsing
- Estimated owners calculation
- MySQL UPSERT handling
- Environment variable configuration
- Logging system
- Retry logic for database startup
- Docker containerization
- Docker Compose orchestration
- Modular project structure

---

## Project Structure

```text
src/
├── api/
│   └── steam_api.py
├── database/
│   └── mysql_database.py
├── transform/
│   └── transform.py
└── main.py

init/
└── init.sql

logs/

Dockerfile
docker-compose.yml
requirements.txt
```

---

## Setup

### Create `.env`

```env
MYSQL_HOST=mysql
MYSQL_USER=root
MYSQL_PASSWORD=rootpassword
MYSQL_DATABASE=steam_pipeline
```

---

## Run With Docker Compose

```bash
docker compose up --build
```

---

## Run Pipeline Only

```bash
docker compose run --rm pipeline
```

---

## Future Improvements

- Airflow orchestration
- Azure deployment
- Power BI dashboard
- Automated scheduling
- Data quality checks
- CI/CD pipeline