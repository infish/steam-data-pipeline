# Steam Data Pipeline

Simple ETL pipeline project built with Python and MySQL.

## Project Overview

This project:

- Extracts data from the SteamSpy API
- Transforms selected game data
- Loads the data into a MySQL database

## Technologies Used:

- Python
- Requests
- MySQL
- MySQL Connector
- Git & GitHub

## Pipeline Flow:

Steam API → Python → Data Transformation → MySQL

## Features:

- API data extraction
- JSON parsing
- MySQL database integration
- UPSERT handling
- Environment variable configuration
- Modular project structure

## Project Structure:

```text
src/
├── api/
│   └── steam_api.py
├── database/
│   └── mysql_database.py
└── main.py
```

## Setup:

Install dependencies:

```bash
pip install -r requirements.txt
```

Create `.env` file:

```env
MYSQL_HOST=localhost
MYSQL_USER=root
MYSQL_PASSWORD=your_password
MYSQL_DATABASE=steam_pipeline
```

Run pipeline:

```bash
python src/main.py
```


## Future Improvements

- Docker support
- Scheduling
- Azure deployment