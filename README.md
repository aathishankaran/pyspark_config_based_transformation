# PySpark Config-Based Transformation Pipeline

A config-driven PySpark pipeline that reads JSON configuration to define inputs, outputs, and transformations. Supports single input/output and many-to-many (multiple named inputs and outputs) with backup patterns and draw.io diagram export.

## Features

- **JSON (or YAML) configuration**: Define inputs, outputs, and transformation steps in one file.
- **Single and many-to-many**: One input → one output, or multiple inputs → multiple outputs with named DataFrames.
- **Transformations**: select, filter, rename, with_column, sort, join, merge, aggregate, date_format, calculation, etc.
- **Formats**: CSV, Parquet, JSON; custom delimited, fixed-width, and VSAM-like binary.
- **Backup**: Optional daily/month-end backup path patterns with `{yyyy}`, `{mm}`, `{dd}` placeholders.
- **Diagram**: Export pipeline flow to draw.io XML.

## Project Structure

**This folder is the repository root** — push this folder alone as one Git repo.

```
pyspark_program_config_based_transformation/
├── README.md
├── requirements.txt
├── .gitignore
├── run_pipeline.py                 # Main entry point
├── pipeline_to_drawio.py           # Export config to draw.io
├── config/                         # All pipeline configs
│   ├── configuration.json
│   ├── configuration_many_to_many.json
│   └── configuration_complex.json
├── src/
│   ├── __init__.py
│   ├── config_loader.py            # Load/validate config
│   ├── transformations.py          # Transformation implementations
│   ├── pipeline.py                 # Run pipeline
│   └── readers.py                  # Delimited, fixed-width, VSAM readers
├── create_vsam_binary.py           # Generate data/prod_customer_vsam.dat for VSAM testing
└── data/                           # Sample input data
    ├── master_customer.csv
    ├── prod_customer_vsam.csv
    ├── prod_customer_vsam.dat      # Binary VSAM (after create_vsam_binary.py)
    ├── sample_input.csv
    ├── sample_customers.csv
    ├── sample_orders.csv
    ├── sample_returns.csv
    └── sample_fixed_width.txt
```

## Requirements

- **Python 3.8+** (for **Python 3.12** on Windows, use default `requirements.txt` — PySpark 4.x is compatible).
- **Java**: **Java 17+** for default `requirements.txt` (PySpark 4.x). For **Java 11**, use `requirements-java11.txt` (PySpark 3.3.x; Python 3.12 not fully supported there).
- **PySpark** (and Java for Spark)
- **PyYAML** (for YAML config). See `requirements.txt`.

### Windows (Python 3.12, Java 17)

On a Windows machine where you must use **Python 3.12** and **Java 17**:

1. **Set Java 17** (Command Prompt or PowerShell, or System Environment Variables):
   ```cmd
   set JAVA_HOME=C:\Program Files\Java\jdk-17
   ```
   (Adjust path if JDK is elsewhere, e.g. `C:\Program Files\Eclipse Adoptium\jdk-17.0.x-hotspot`.)

2. **Create a venv and install deps** (project folder):
   ```cmd
   cd pyspark_program_config_based_transformation
   python -m venv .venv
   .venv\Scripts\activate
   pip install -r requirements.txt
   ```
   PySpark 4.x in `requirements.txt` works with Python 3.12 (no `typing.io` error).

3. **Run the pipeline** (choose one):

   **Option A – Batch file (recommended on Windows if you see worker/serializer errors)**  
   Sets `PYSPARK_PYTHON` to the full path of the venv Python before starting the app. Try in this order:
   ```cmd
   run_pipeline_direct.bat
   run_pipeline_direct.bat config\generated_configuration.json
   ```
   If that still fails, try:
   ```cmd
   run_with_spark_submit.bat
   run_with_spark_submit.bat config\generated_configuration.json
   ```

   **Option B – Direct spark-submit (if you have a Spark installation)**  
   Set `SPARK_HOME` to your Spark directory (e.g. `C:\Spark\spark-4.0.0`). Use the **full path** to your venv Python (no spaces in path is best):
   ```cmd
   set SPARK_HOME=C:\Spark\spark-4.0.0
   set VENV_PYTHON=C:\workspace\code_base\pyspark_config_based_transformation\.venv\Scripts\python.exe
   %SPARK_HOME%\bin\spark-submit --master local[1] --conf spark.pyspark.python=%VENV_PYTHON% --conf spark.pyspark.driver.python=%VENV_PYTHON% run_pipeline.py -c config\generated_configuration.json
   ```
   Replace `VENV_PYTHON` and paths with your actual project and venv location.

   **Option C – Python (if no worker errors)**:
   ```cmd
   python run_pipeline.py -c config\generated_configuration.json
   ```
   Config paths can use either backslashes or forward slashes.

### Why this often fails on Windows

PySpark runs a **driver** (your script) and **worker** processes (launched by Spark’s JVM). On Windows several things go wrong:

1. **Worker Python ≠ driver Python**  
   The JVM starts worker processes by running “python” (or the path in `PYSPARK_PYTHON`). If that points to a different Python than the one running your script (e.g. system vs venv), or if the env var isn’t passed correctly, workers and driver get out of sync → **serializer / `read_int`** errors.

2. **Paths with spaces**  
   Project paths like `C:\...\Git Project\...` or `Program Files` can break the command the JVM uses to start the worker. The executable path is often not quoted correctly, so the worker never starts or fails immediately.

3. **No fork()**  
   On Linux/macOS, Spark can rely on process semantics that Windows doesn’t have. On Windows everything uses subprocess + sockets; any env or path issue shows up as worker/communication failures.

4. **When `PYSPARK_PYTHON` is set**  
   Setting it **inside** your Python script (e.g. in `run_pipeline.py`) only affects the driver. The JVM that launches workers was already started by then and may have captured a different environment. So it must be set **before** starting the driver (e.g. in a `.bat` or in the terminal).

**What usually fixes it**

- Use a **project path with no spaces** (e.g. `C:\code\pyspark_config` instead of `C:\workspace\Git Project\...`).
- Run via **`run_pipeline_direct.bat`** or **`run_with_spark_submit.bat`** so `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` are set to the **full path** of `.venv\Scripts\python.exe` in the same shell that starts the app.
- If you use **spark-submit**, pass the venv Python path explicitly:  
  `--conf spark.pyspark.python=...\\.venv\\Scripts\\python.exe` (and same for `spark.pyspark.driver.python`).
- As a last resort, run the pipeline **inside WSL** (same code, Linux environment) so Spark behaves like on Linux.

### Java version (macOS / Linux)

Check your Java version:

```bash
java -version
# Need: openjdk version "17" or higher
```

If you have Java 11 and cannot upgrade system-wide, install Java 17 and point PySpark to it:

```bash
# macOS (Homebrew)
brew install openjdk@17
export JAVA_HOME=/opt/homebrew/opt/openjdk@17   # Apple Silicon
# export JAVA_HOME=/usr/local/opt/openjdk@17    # Intel

# Then run the pipeline
python run_pipeline.py --config config/generated_configuration.json
```

On Linux, use your distro’s OpenJDK 17 package or a tarball and set `JAVA_HOME` accordingly.

## Setup

```bash
cd pyspark_program_config_based_transformation
pip install -r requirements.txt
```

If the project runs in one folder but fails with a Java version error in a **copied** folder (e.g. different machine or new clone), either set `JAVA_HOME` to Java 17 in that folder or install the Java-11–compatible stack there:

```bash
pip install -r requirements-java11.txt
```

## Usage

Run from the **project root** (`pyspark_program_config_based_transformation/`).

### Commands

```bash
# Default config (config/configuration.json)
python run_pipeline.py

# Custom config (short or long form)
python run_pipeline.py --config config/configuration_many_to_many.json
python run_pipeline.py -c config/complete_configuration.json

# Configs included: configuration.json, configuration_many_to_many.json,
# configuration_complex.json, configuration_vsam_input.json, complete_configuration.json
python run_pipeline.py -c config/configuration_vsam_input.json

# Export pipeline to draw.io
python pipeline_to_drawio.py --config config/configuration.json -o pipeline.drawio
```

### Options

| Option | Description |
|--------|-------------|
| `--config`, `-c` | Path to configuration file (JSON or YAML). Default: `config/configuration.json` or `CONFIG_PATH` env. |

```bash
# Using environment variable
CONFIG_PATH=config/configuration_many_to_many.json python run_pipeline.py

# Show all options
python run_pipeline.py --help
```

## Configuration

- **Single mode**: `input`, `output`, `transformations`.
- **Many-to-many**: `inputs` (name → path/format/options), `outputs` (name → path, `dataframe`, format, backup?), `transformations` (with `dataframe`/`output` and `left`/`right` for join/merge).

See `config/configuration.json` and `config/configuration_many_to_many.json` for examples.

## Reading VSAM (binary) input

You can read **VSAM-style binary** files (fixed-length records, EBCDIC, COMP-3) by setting an input’s `format` to `"vsam"` and providing layout options.

1. **Create the binary file** (same data as `data/prod_customer_vsam.csv`, 61 bytes/record):

   ```bash
   python create_vsam_binary.py
   ```
   This writes `data/prod_customer_vsam.dat`.

2. **Use a config that uses VSAM** for that input, for example `config/configuration_vsam_input.json`:

   ```bash
   python run_pipeline.py --config config/configuration_vsam_input.json
   ```

3. **VSAM input options** (under `inputs.<name>.options` when `format` is `"vsam"`):
   - `lrecl`: record length in bytes (e.g. 61)
   - `ebcdic_codepage`: e.g. `"cp037"`
   - `columns`: array of `{ "name", "start", "length", "type" }` (0-based; `type` is `"string"` or `"comp3"`/`"decimal"`)

## Troubleshooting

- **`ModuleNotFoundError: No module named 'typing.io'`** (Python 3.12): Older PySpark (3.4.x) used `typing.io`, which was removed in Python 3.12. Use **PySpark 4.x**: `pip install -r requirements.txt` (this project pins `pyspark>=4.0.0`). See [Windows (Python 3.12, Java 17)](#windows-python-312-java-17).

- **`UnsupportedClassVersionError` (class file version 61.0 / 55.0)** or **`JAVA_GATEWAY_EXITED`**: PySpark 4.x needs **Java 17**. Your `java -version` is likely 11. Set `JAVA_HOME` to Java 17 and run again. See [Java version](#java-version) or [Windows](#windows-python-312-java-17).

- **`pyspark\serializers.py` / `read_int` or worker crash on Windows**: Usually the Spark worker is using a different Python than the driver. This project sets `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` to the current interpreter and uses `local[1]` and a fixed `spark.local.dir` on Windows. If you still see it, run from the project root with the venv activated: `python run_pipeline.py -c config\your_config.json` (do not use `python -m src.pipeline` from another cwd if it changes which Python is used).

- **Runs in one folder but fails when you copy the project elsewhere**: Each copy has its own environment.
  - The **working** folder may be using Java 17 (e.g. `JAVA_HOME` set in that terminal or IDE), or an older PySpark (3.3.x) that supports Java 11.
  - The **copied** folder usually has a **new venv** and `pip install -r requirements.txt` installs **PySpark 4.x**, which requires Java 17 and supports Python 3.12. That copy might also be opening in a different terminal/IDE where `java` is still 11.
  - **Fix (choose one):**
    1. **Use Java 17 in the copied folder too:** In the copied project’s terminal, set `JAVA_HOME` to Java 17 (see [Java version](#java-version)) and run again. No code changes.
    2. **Use Java 11 in both places:** In the copied project run `pip install -r requirements-java11.txt` (or `pyspark>=3.3,<3.4`) so PySpark 3.3.x is used; it supports Java 11. See `requirements-java11.txt`.

## License

Use as needed for your projects.
