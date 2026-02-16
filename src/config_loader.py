"""
Load and validate pipeline configuration from JSON (or YAML).
"""
import json
import os
from pathlib import Path
from typing import Any

def _load_json(path: Path) -> dict[str, Any]:
    with open(path, "r") as f:
        return json.load(f)


def _load_yaml(path: Path) -> dict[str, Any]:
    import yaml
    with open(path, "r") as f:
        return yaml.safe_load(f)


def load_config(config_path: str | None = None) -> dict[str, Any]:
    """
    Load configuration from a JSON or YAML file.
    Default path: config/configuration.json in project root, or CONFIG_PATH env var.
    Format is inferred from file extension (.json -> JSON, otherwise YAML).
    """
    if config_path is None:
        config_path = os.environ.get("CONFIG_PATH", "config/configuration.json")
    path = Path(config_path)
    if not path.is_absolute():
        cwd = Path.cwd()
        for base in (cwd, cwd.parent):
            candidate = base / path
            if candidate.exists():
                path = candidate
                break
        else:
            path = cwd / config_path
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")
    if path.suffix.lower() == ".json":
        config = _load_json(path)
    else:
        config = _load_yaml(path)
    if not config:
        raise ValueError("Configuration file is empty")
    return config


def get_input_config(config: dict[str, Any]) -> dict[str, Any]:
    """Extract and validate input section."""
    inp = config.get("input") or {}
    if not inp.get("path"):
        raise ValueError("config.input.path is required")
    return {
        "path": inp["path"],
        "format": inp.get("format", "csv"),
        "options": inp.get("options") or {},
    }


def get_output_config(config: dict[str, Any]) -> dict[str, Any]:
    """Extract and validate output section."""
    out = config.get("output") or {}
    if not out.get("path"):
        raise ValueError("config.output.path is required")
    return {
        "path": out["path"],
        "format": out.get("format", "csv"),
        "mode": out.get("mode", "overwrite"),
        "options": out.get("options") or {},
        "backup": out.get("backup"),
    }


def get_transformations(config: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract transformations list (order preserved)."""
    trans = config.get("transformations")
    if trans is None:
        return []
    if not isinstance(trans, list):
        raise ValueError("config.transformations must be a list")
    return trans


def get_app_name(config: dict[str, Any]) -> str:
    """Application name for Spark."""
    return config.get("app_name") or "PySpark-Config-Pipeline"


def is_many_to_many(config: dict[str, Any]) -> bool:
    """True if config uses multiple inputs and multiple outputs (many-to-many)."""
    return bool(config.get("inputs") and config.get("outputs"))


def get_inputs_config(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """
    Extract named inputs for many-to-many mode.
    Returns dict: name -> { path, format, options }.
    """
    inputs = config.get("inputs")
    if not inputs or not isinstance(inputs, dict):
        raise ValueError("config.inputs must be an object mapping name -> { path, format?, options? }")
    result = {}
    for name, cfg in inputs.items():
        if not isinstance(cfg, dict) or not cfg.get("path"):
            raise ValueError(f"config.inputs.{name} must have a 'path'")
        result[name] = {
            "path": cfg["path"],
            "format": cfg.get("format", "csv"),
            "options": cfg.get("options") or {},
        }
    return result


def get_outputs_config(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """
    Extract named outputs for many-to-many mode.
    Returns dict: output_name -> { path, format, mode, options, dataframe }.
    Each output writes the named 'dataframe' (from transformations) to path.
    """
    outputs = config.get("outputs")
    if not outputs or not isinstance(outputs, dict):
        raise ValueError("config.outputs must be an object mapping name -> { path, dataframe, format?, mode?, options? }")
    result = {}
    for name, cfg in outputs.items():
        if not isinstance(cfg, dict) or not cfg.get("path"):
            raise ValueError(f"config.outputs.{name} must have a 'path'")
        df_name = cfg.get("dataframe")
        if not df_name:
            raise ValueError(f"config.outputs.{name} must have a 'dataframe' (name of DataFrame to write)")
        result[name] = {
            "path": cfg["path"],
            "format": cfg.get("format", "csv"),
            "mode": cfg.get("mode", "overwrite"),
            "options": cfg.get("options") or {},
            "dataframe": df_name,
            "backup": cfg.get("backup"),
        }
    return result
