@echo off
echo ============================================
echo   Storage Tools — Web Server
echo ============================================
echo.

cd /d "%~dp0"

:: Load .env file if it exists
if exist .env (
    for /f "usebackq tokens=1,* delims==" %%a in (".env") do (
        if not "%%a"=="" if not "%%a:~0,1%"=="#" set "%%a=%%b"
    )
)

:: Check for API key
if "%ANTHROPIC_API_KEY%"=="" (
    echo ERROR: ANTHROPIC_API_KEY not set.
    echo Create a .env file with your API key. See .env.example
    pause
    exit /b 1
)

echo Starting server at http://localhost:8000
echo Press Ctrl+C to stop.
echo.
python -m uvicorn app:app --host 0.0.0.0 --port 8000 --reload
pause
