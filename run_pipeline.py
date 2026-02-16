#!/usr/bin/env python3
"""
Entry point for the config-driven PySpark pipeline.
Run from project root:
  python run_pipeline.py
  python run_pipeline.py --config config/configuration.json
  spark-submit run_pipeline.py
"""
import os
import sys

# Avoid Java 9+ / Hadoop "getSubject is not supported" when running locally
if "HADOOP_USER_NAME" not in os.environ:
    os.environ["HADOOP_USER_NAME"] = os.environ.get("USER", os.environ.get("USERNAME", "unknown"))

# Ensure project root is on path
_project_root = os.path.dirname(os.path.abspath(__file__))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.pipeline import main

if __name__ == "__main__":
    main()
