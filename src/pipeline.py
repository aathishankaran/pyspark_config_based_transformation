"""
PySpark config-driven pipeline.
Reads input/output paths and transformation list from configuration.json,
runs all steps, and writes the final result.

Usage:
  python -m src.pipeline
  python -m src.pipeline --config /path/to/configuration.json
  CONFIG_PATH=/path/to/configuration.json python -m src.pipeline
"""
import argparse
import os
import sys
from datetime import date

from pyspark.sql import SparkSession

# Add project root so config_loader finds config/configuration.json
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.config_loader import (
    load_config,
    get_app_name,
    get_input_config,
    get_output_config,
    get_inputs_config,
    get_outputs_config,
    get_transformations,
    is_many_to_many,
)
from src.transformations import (
    apply_all_transformations,
    apply_transformation,
    apply_binary_transformation,
)
from src.readers import read_delimited, read_fixed_width, read_vsam


def create_spark_session(app_name: str) -> SparkSession:
    """Build Spark session with sensible defaults. Uses local master to avoid Hadoop UGI/Subject issues on Java 9+."""
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
    )
    # Local mode: avoid Hadoop security (getSubject) issues on Java 17+
    try:
        builder = builder.master("local[*]")
    except Exception:
        pass
    return builder.getOrCreate()


def read_input(spark: SparkSession, input_config: dict):
    """
    Read DataFrame from configured input path and format.
    Formats: csv, parquet, json (Spark native); delimited, fixed_width, vsam (custom readers).
    """
    path = input_config["path"]
    fmt = input_config["format"].lower().strip()
    options = input_config.get("options") or {}

    if fmt == "delimited":
        return read_delimited(spark, path, options)
    if fmt == "fixed_width":
        return read_fixed_width(spark, path, options)
    if fmt == "vsam":
        return read_vsam(spark, path, options)
    # Native Spark formats
    reader = spark.read.format(fmt)
    for key, value in options.items():
        reader = reader.option(key, value)
    return reader.load(path)


def _resolve_backup_path(pattern: str) -> str:
    """Replace {yyyy}, {mm}, {dd}, {date}, {year}, {month} in path pattern."""
    today = date.today()
    return (
        pattern.replace("{yyyy}", str(today.year))
        .replace("{year}", str(today.year))
        .replace("{mm}", f"{today.month:02d}")
        .replace("{month}", f"{today.month:02d}")
        .replace("{dd}", f"{today.day:02d}")
        .replace("{date}", today.isoformat())
    )


def _is_month_end() -> bool:
    """True if today is the last day of the month."""
    from datetime import timedelta
    today = date.today()
    tomorrow = today + timedelta(days=1)
    return tomorrow.month != today.month


def _write_to_path(df, path: str, fmt: str, mode: str, options: dict) -> None:
    """Write DataFrame to a single path."""
    writer = df.write.format(fmt).mode(mode)
    for key, value in (options or {}).items():
        writer = writer.option(key, value)
    writer.save(path)


def write_output(df, output_config: dict) -> None:
    """
    Write DataFrame to configured output path and format.
    Optional backup: backup.daily and backup.month_end path patterns (with {yyyy},{mm},{dd},{date}).
    Month-end backup is written only on the last day of the month.
    """
    path = output_config["path"]
    fmt = output_config["format"].lower()
    mode = output_config.get("mode", "overwrite")
    options = output_config.get("options") or {}

    _write_to_path(df, path, fmt, mode, options)

    backup = output_config.get("backup") or {}
    if isinstance(backup, dict):
        daily = backup.get("daily")
        if daily:
            backup_path = _resolve_backup_path(daily)
            _write_to_path(df, backup_path, fmt, mode, options)
            print(f"  Backup (daily): {backup_path}")
        month_end = backup.get("month_end")
        if month_end and _is_month_end():
            backup_path = _resolve_backup_path(month_end)
            _write_to_path(df, backup_path, fmt, mode, options)
            print(f"  Backup (month-end): {backup_path}")


def run_pipeline_many_to_many(spark: SparkSession, config: dict) -> None:
    """Run pipeline with multiple inputs and multiple outputs."""
    inputs_config = get_inputs_config(config)
    outputs_config = get_outputs_config(config)
    transformations = get_transformations(config)

    # Load all inputs as named DataFrames
    dataframes = {}
    for name, cfg in inputs_config.items():
        dataframes[name] = read_input(spark, cfg)
        print(f"Input '{name}': {cfg['path']} (format={cfg['format']}) -> {dataframes[name].count()} rows")

    # Apply transformations
    for step in transformations:
        step_type = step.get("type")
        if step.get("left") is not None and step.get("right") is not None:
            # Binary: join or union
            out_name = step.get("output")
            if not out_name:
                raise ValueError("Transformation step with left/right must have 'output'")
            left_name = step["left"]
            right_name = step["right"]
            if left_name not in dataframes or right_name not in dataframes:
                raise ValueError(f"Unknown dataframe: '{left_name}' or '{right_name}'")
            result = apply_binary_transformation(dataframes[left_name], dataframes[right_name], step)
            dataframes[out_name] = result
            print(f"  -> {step_type} ({left_name} + {right_name}) -> '{out_name}'")
        else:
            # Single DataFrame
            df_name = step.get("dataframe")
            if not df_name:
                raise ValueError("Transformation step must have 'dataframe' or (left + right)")
            out_name = step.get("output", df_name)
            if df_name not in dataframes:
                raise ValueError(f"Unknown dataframe: '{df_name}'")
            result = apply_transformation(dataframes[df_name], step)
            dataframes[out_name] = result
            print(f"  -> {step_type} on '{df_name}' -> '{out_name}'")

    # Write each output
    for out_key, out_cfg in outputs_config.items():
        df_name = out_cfg["dataframe"]
        if df_name not in dataframes:
            raise ValueError(f"Output '{out_key}' references unknown dataframe '{df_name}'")
        write_output(dataframes[df_name], out_cfg)
        print(f"Output '{out_key}': {out_cfg['path']} (dataframe={df_name})")
        dataframes[df_name].show(5, truncate=False)


def run_pipeline(config_path: str | None = None) -> None:
    """Load config, run read -> transform -> write (single or many-to-many)."""
    config = load_config(config_path)
    app_name = get_app_name(config)
    spark = create_spark_session(app_name)

    if is_many_to_many(config):
        run_pipeline_many_to_many(spark, config)
    else:
        input_config = get_input_config(config)
        output_config = get_output_config(config)
        transformations = get_transformations(config)

        df = read_input(spark, input_config)
        print(f"Input: {input_config['path']} (format={input_config['format']})")
        print(f"Rows read: {df.count()}")

        if transformations:
            df = apply_all_transformations(df, transformations)
            print(f"Applied {len(transformations)} transformation(s).")
        else:
            print("No transformations configured.")

        write_output(df, output_config)
        print(f"Output written to: {output_config['path']} (format={output_config['format']}, mode={output_config.get('mode', 'overwrite')})")
        print("\nSample of final result:")
        df.show(20, truncate=False)

    spark.stop()


def main():
    parser = argparse.ArgumentParser(description="Run PySpark pipeline from YAML config")
    parser.add_argument(
        "--config",
        "-c",
        default=os.environ.get("CONFIG_PATH", "config/configuration.json"),
        help="Path to configuration file (JSON or YAML)",
    )
    args = parser.parse_args()
    run_pipeline(config_path=args.config)


if __name__ == "__main__":
    main()
