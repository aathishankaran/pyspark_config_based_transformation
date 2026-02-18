@echo off
REM Run the pipeline via spark-submit so driver and workers use the same env (avoids read_int / serializer errors on Windows).
REM Edit JAVA_HOME below if needed. Run from project root: run_with_spark_submit.bat
REM Optional: run_with_spark_submit.bat config\other_config.json

set "PROJECT_ROOT=%~dp0"
set "PROJECT_ROOT=%PROJECT_ROOT:~0,-1%"
cd /d "%PROJECT_ROOT%"

set "VENV_PYTHON=%PROJECT_ROOT%\.venv\Scripts\python.exe"
if not exist "%VENV_PYTHON%" (
  echo ERROR: .venv not found. Create it: python -m venv .venv & .venv\Scripts\activate & pip install -r requirements.txt
  exit /b 1
)

REM Force driver and workers to use the same Python (full path)
set "PYSPARK_PYTHON=%VENV_PYTHON%"
set "PYSPARK_DRIVER_PYTHON=%VENV_PYTHON%"

REM Optional: set if Java 17 is not on PATH
REM set "JAVA_HOME=C:\Program Files\Java\jdk-17"

set "CONFIG=%~1"
if "%CONFIG%"=="" set "CONFIG=config\generated_configuration.json"

REM Find spark-submit from pip-installed PySpark
for /f "delims=" %%i in ('"%VENV_PYTHON%" -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'bin'))" 2^>nul') do set "PYSERVER_BIN=%%i"
if not defined PYSERVER_BIN (
  echo PySpark not found in venv. Run: pip install -r requirements.txt
  exit /b 1
)

REM Use spark-submit.cmd if present, else spark-submit (Unix script may work with Python)
set "SPARK_SUBMIT=%PYSERVER_BIN%\spark-submit.cmd"
if not exist "%SPARK_SUBMIT%" set "SPARK_SUBMIT=%PYSERVER_BIN%\spark-submit"
if not exist "%SPARK_SUBMIT%" (
  echo spark-submit not found in %PYSERVER_BIN%. Use direct command below.
  echo Running with python instead and PYSPARK_PYTHON set...
  "%VENV_PYTHON%" run_pipeline.py -c "%CONFIG%"
  exit /b %ERRORLEVEL%
)

echo Using: %SPARK_SUBMIT%
echo Config: %CONFIG%
"%SPARK_SUBMIT%" --master local[1] run_pipeline.py -c "%CONFIG%"
exit /b %ERRORLEVEL%
