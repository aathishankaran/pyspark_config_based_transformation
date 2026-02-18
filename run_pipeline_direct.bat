@echo off
REM Set PYSPARK_PYTHON to full path of venv Python so workers use same interpreter (avoids read_int on Windows).
REM Run from project root: run_pipeline_direct.bat
REM Optional: run_pipeline_direct.bat config\other_config.json

set "PROJECT_ROOT=%~dp0"
set "PROJECT_ROOT=%PROJECT_ROOT:~0,-1%"
cd /d "%PROJECT_ROOT%"

set "VENV_PYTHON=%PROJECT_ROOT%\.venv\Scripts\python.exe"
if not exist "%VENV_PYTHON%" (
  echo ERROR: .venv not found at %VENV_PYTHON%
  exit /b 1
)

set "PYSPARK_PYTHON=%VENV_PYTHON%"
set "PYSPARK_DRIVER_PYTHON=%VENV_PYTHON%"

set "CONFIG=%~1"
if "%CONFIG%"=="" set "CONFIG=config\generated_configuration.json"

"%VENV_PYTHON%" run_pipeline.py -c "%CONFIG%"
exit /b %ERRORLEVEL%
