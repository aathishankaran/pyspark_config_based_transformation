"""
Custom readers for delimited, fixed-width, and VSAM (binary) sources.
Used by the pipeline when input format is delimited, fixed_width, or vsam.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType
from typing import Any


def _unpack_comp3(b: bytes) -> float:
    """Unpack IBM COMP-3 (packed decimal) bytes to float."""
    if not b or len(b) == 0:
        return 0.0
    nibbles = []
    for byte in b:
        nibbles.append((byte >> 4) & 0x0F)
        nibbles.append(byte & 0x0F)
    if nibbles:
        sign_nibble = nibbles.pop()
        negative = sign_nibble in (0x0B, 0x0D)
    else:
        negative = False
    value = 0
    for n in nibbles:
        if 0 <= n <= 9:
            value = value * 10 + n
    return (-value / 100.0) if negative else (value / 100.0)


def read_delimited(spark: SparkSession, path: str, options: dict) -> Any:
    """
    Read delimited text (CSV, TSV, pipe, etc.).
    options: header (bool), delimiter (str), inferSchema (bool), etc.
    """
    opts = {k: str(v) if not isinstance(v, bool) else v for k, v in (options or {}).items()}
    reader = spark.read.format("csv")
    for k, v in opts.items():
        reader = reader.option(k, v)
    return reader.load(path)


def read_fixed_width(spark: SparkSession, path: str, options: dict) -> Any:
    """
    Read fixed-width text. Each line is split by column positions.
    options: columns (list of { name, start, length } or { name, width }). start 1-based.
    """
    columns_cfg = (options or {}).get("columns", [])
    if not columns_cfg:
        raise ValueError("fixed_width format requires options.columns (name, start, length)")
    # Read as text (one column "value")
    df = spark.read.text(path)
    from pyspark.sql import functions as F
    # Build select: substring(value, start, length). Spark substring is 1-based.
    select_exprs = []
    for c in columns_cfg:
        start = c.get("start", 1)
        length_val = c.get("length") or c.get("width", 1)
        name = c.get("name", "col")
        select_exprs.append(F.substring(F.col("value"), start, length_val).cast(StringType()).alias(name))
    return df.select(*select_exprs)


def _vsam_parse_record(content: bytes, layout: list, ebcdic: str, lrecl: int) -> list:
    """Parse one fixed-length binary record into a list of values. Layout: [{name, start, length, type}]."""
    if content is None or len(content) < lrecl:
        return [None] * len(layout)
    row = []
    for col_def in layout:
        start = col_def.get("start", 0)
        length_val = col_def.get("length", 1)
        col_type = (col_def.get("type") or "string").lower()
        end = start + length_val
        chunk = bytes(content[start:end]) if end <= len(content) else b""
        if col_type == "decimal" or col_type == "comp3":
            row.append(_unpack_comp3(chunk))
        else:
            try:
                row.append(chunk.decode(ebcdic, errors="replace").strip())
            except Exception:
                row.append(None)
    return row


def read_vsam(spark: SparkSession, path: str, options: dict) -> Any:
    """
    Read VSAM-like binary (fixed-length records, EBCDIC, optional COMP-3).
    options: lrecl (int), ebcdic_codepage (str, default cp037), columns: [{ name, start, length, type?: string|decimal }].
    start/length 0-based byte positions.
    """
    lrecl = int((options or {}).get("lrecl", 80))
    ebcdic = (options or {}).get("ebcdic_codepage", "cp037")
    layout = (options or {}).get("columns", [])
    if not layout:
        raise ValueError("vsam format requires options.columns (name, start, length, type?)")
    # Build schema from layout
    from pyspark.sql.types import StructField
    fields = []
    for c in layout:
        col_type = (c.get("type") or "string").lower()
        if col_type in ("decimal", "comp3"):
            fields.append(StructField(c["name"], DoubleType(), True))
        else:
            fields.append(StructField(c["name"], StringType(), True))
    record_schema = StructType(fields)
    # UDF: bytes -> list of tuples (one per record)
    def parse(content):
        if content is None:
            content = b""
        content = bytes(content)
        out = []
        for i in range(0, len(content), lrecl):
            chunk = content[i : i + lrecl]
            if len(chunk) < lrecl:
                break
            out.append(tuple(_vsam_parse_record(chunk, layout, ebcdic, lrecl)))
        return out
    from pyspark.sql.functions import udf
    parse_udf = udf(parse, ArrayType(record_schema))
    binary_df = (
        spark.read.format("binaryFile")
        .option("pathGlobFilter", "*")
        .option("recursiveFileLookup", "true")
        .load(path)
    )
    records_df = binary_df.withColumn("records", parse_udf(col("content")))
    df = records_df.withColumn("record", explode(col("records")))
    select_cols = [col("record." + f.name).alias(f.name) for f in record_schema.fields]
    df = df.select(*select_cols)
    return df
