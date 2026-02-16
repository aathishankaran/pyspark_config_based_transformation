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

- **Python 3.8+**
- **PySpark** (and Java for Spark)
- **PyYAML** (for YAML config). See `requirements.txt`.

## Setup

```bash
cd pyspark_program_config_based_transformation
pip install -r requirements.txt
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


## License

Use as needed for your projects.
