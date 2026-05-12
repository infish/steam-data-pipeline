@echo off
setlocal

cd /d "%~dp0"

where py >nul 2>nul
if errorlevel 1 (
    python -m venv venv
) else (
    py -3.10 -m venv venv
    if errorlevel 1 (
        py -m venv venv
    )
)

if not exist "venv\Scripts\python.exe" (
    echo Could not create venv. Install Python 3.10+ and try again.
    pause
    exit /b 1
)

venv\Scripts\python.exe -m pip install --upgrade pip
venv\Scripts\python.exe -m pip install -r requirements.txt

echo Virtual environment is ready.
pause
