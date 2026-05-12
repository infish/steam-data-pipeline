@echo off
setlocal

cd /d "%~dp0"

docker compose up -d mysql
if errorlevel 1 (
    echo Failed to start MySQL container.
    exit /b 1
)

docker compose run --rm pipeline
if errorlevel 1 (
    echo Pipeline container failed.
    exit /b 1
)

echo Docker pipeline completed successfully.
