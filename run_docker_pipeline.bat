@echo off
setlocal

cd /d "%~dp0"

if not exist "logs" mkdir logs

set "LOG_FILE=logs\scheduler.log"

echo.>> "%LOG_FILE%"
echo ========================================>> "%LOG_FILE%"
echo Pipeline scheduled run started at %DATE% %TIME%>> "%LOG_FILE%"
echo Working directory: %CD%>> "%LOG_FILE%"

call :ensure_docker_ready
if errorlevel 1 (
    echo Docker was not ready.>> "%LOG_FILE%"
    exit /b 1
)

docker compose up -d mysql >> "%LOG_FILE%" 2>>&1
if errorlevel 1 (
    echo Failed to start MySQL container.>> "%LOG_FILE%"
    exit /b 1
)

docker compose run --rm pipeline >> "%LOG_FILE%" 2>>&1
if errorlevel 1 (
    echo Pipeline container failed.>> "%LOG_FILE%"
    exit /b 1
)

echo Docker pipeline completed successfully at %DATE% %TIME%>> "%LOG_FILE%"
exit /b 0

:ensure_docker_ready
docker info >> "%LOG_FILE%" 2>>&1
if not errorlevel 1 (
    echo Docker is ready.>> "%LOG_FILE%"
    exit /b 0
)

echo Docker is not ready. Trying to start Docker Desktop...>> "%LOG_FILE%"

set "DOCKER_DESKTOP=C:\Program Files\Docker\Docker\Docker Desktop.exe"

if exist "%DOCKER_DESKTOP%" (
    start "" "%DOCKER_DESKTOP%"
) else (
    echo Docker Desktop executable not found at %DOCKER_DESKTOP%.>> "%LOG_FILE%"
)

for /l %%i in (1,1,30) do (
    timeout /t 5 /nobreak >nul
    docker info >> "%LOG_FILE%" 2>>&1
    if not errorlevel 1 (
        echo Docker became ready after %%i wait attempts.>> "%LOG_FILE%"
        exit /b 0
    )
    echo Waiting for Docker Desktop... attempt %%i/30>> "%LOG_FILE%"
)

exit /b 1
