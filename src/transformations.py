"""
Dynamic PySpark transformations driven by configuration.
Each transformation type is registered and applied by name.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def apply_select(df: DataFrame, params: dict) -> DataFrame:
    """Select columns by name."""
    columns = params.get("columns")
    if not columns:
        return df
    return df.select(*[F.col(c) for c in columns])


def apply_drop(df: DataFrame, params: dict) -> DataFrame:
    """Drop columns by name."""
    columns = params.get("columns")
    if not columns:
        return df
    return df.drop(*columns)


def apply_filter(df: DataFrame, params: dict) -> DataFrame:
    """Filter rows using a SQL expression string."""
    condition = params.get("condition")
    if not condition:
        return df
    return df.filter(condition)


def apply_with_column(df: DataFrame, params: dict) -> DataFrame:
    """Add or replace a column using a SQL expression."""
    name = params.get("name")
    expr = params.get("expr")
    if not name or expr is None:
        return df
    return df.withColumn(name, F.expr(expr))


def apply_rename(df: DataFrame, params: dict) -> DataFrame:
    """Rename columns. params['columns'] is a dict: old_name -> new_name."""
    mapping = params.get("columns") or {}
    for old_name, new_name in mapping.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df


def apply_sort(df: DataFrame, params: dict) -> DataFrame:
    """Sort by columns. params['columns'] is a list of {column, ascending} or column names."""
    columns_cfg = params.get("columns") or []
    if not columns_cfg:
        return df
    cols = []
    for c in columns_cfg:
        if isinstance(c, str):
            cols.append(F.col(c).asc())
        else:
            col_name = c.get("column")
            ascending = c.get("ascending", True)
            cols.append(F.col(col_name).asc() if ascending else F.col(col_name).desc())
    return df.orderBy(*cols)


def apply_drop_duplicates(df: DataFrame, params: dict) -> DataFrame:
    """Drop duplicate rows, optionally considering only subset of columns."""
    subset = params.get("subset")
    if subset:
        return df.dropDuplicates(subset)
    return df.dropDuplicates()


def apply_limit(df: DataFrame, params: dict) -> DataFrame:
    """Limit number of rows."""
    n = params.get("n")
    if n is not None:
        return df.limit(int(n))
    return df


def apply_with_columns(df: DataFrame, params: dict) -> DataFrame:
    """Add or replace multiple columns. params['columns'] is dict: name -> expr."""
    columns = params.get("columns") or {}
    for name, expr in columns.items():
        df = df.withColumn(name, F.expr(expr))
    return df


def apply_join(left: DataFrame, right: DataFrame, params: dict) -> DataFrame:
    """
    Join two DataFrames. Params: on (column name or list), how (inner, left, right, full, etc.).
    """
    on = params.get("on")
    how = params.get("how", "inner")
    if on is None:
        raise ValueError("join transformation requires params.on (column name or list)")
    return left.join(right, on=on, how=how)


def apply_union(left: DataFrame, right: DataFrame, params: dict) -> DataFrame:
    """
    Union two DataFrames. Params: allow_missing_columns (bool) for unionByName.
    """
    allow_missing = params.get("allow_missing_columns", False)
    if allow_missing:
        return left.unionByName(right, allowMissingColumns=True)
    return left.union(right)


def apply_date_format(df: DataFrame, params: dict) -> DataFrame:
    """
    Format date/timestamp column(s). Params: column or columns (list), output_format (str),
    input_format (str, optional when source is string), source_type (date|timestamp|string).
    """
    cols = params.get("columns") or (params.get("column") and [params["column"]] or [])
    if not cols:
        return df
    default_out_fmt = params.get("output_format") or params.get("format") or "yyyy-MM-dd"
    default_in_fmt = params.get("input_format")
    source_type = (params.get("source_type") or "string").lower()
    for c in cols:
        col_name = c if isinstance(c, str) else c.get("name")
        if not col_name:
            continue
        out_col = params.get("output_column") or col_name
        out_fmt = default_out_fmt
        in_fmt = default_in_fmt
        if isinstance(c, dict):
            out_col = c.get("output_column", out_col)
            out_fmt = c.get("output_format") or out_fmt
            in_fmt = c.get("input_format") or in_fmt
        if source_type == "string" and in_fmt:
            df = df.withColumn(out_col, F.date_format(F.to_date(F.col(col_name), in_fmt), out_fmt))
        else:
            df = df.withColumn(out_col, F.date_format(F.col(col_name), out_fmt))
    return df


def apply_aggregate(df: DataFrame, params: dict) -> DataFrame:
    """
    Group by and aggregate. Params: group_by (list of columns), aggs (dict col -> sum|avg|min|max|count|first|last).
    Example: group_by: [region], aggs: { amount: avg, id: count }
    """
    group_cols = params.get("group_by") or params.get("groupBy") or []
    aggs = params.get("aggs") or params.get("aggregations") or {}
    if not group_cols:
        return df
    agg_exprs = []
    for col_name, func in aggs.items():
        func = (func or "sum").lower()
        c = F.col(col_name)
        if func == "avg" or func == "mean":
            agg_exprs.append(F.avg(c).alias(f"avg_{col_name}"))
        elif func == "sum":
            agg_exprs.append(F.sum(c).alias(f"sum_{col_name}"))
        elif func == "min":
            agg_exprs.append(F.min(c).alias(f"min_{col_name}"))
        elif func == "max":
            agg_exprs.append(F.max(c).alias(f"max_{col_name}"))
        elif func == "count":
            agg_exprs.append(F.count(c).alias(f"count_{col_name}"))
        elif func == "first":
            agg_exprs.append(F.first(c).alias(col_name))
        elif func == "last":
            agg_exprs.append(F.last(c).alias(col_name))
        else:
            agg_exprs.append(F.sum(c).alias(f"sum_{col_name}"))
    if not agg_exprs:
        return df.groupBy(*group_cols).count()
    return df.groupBy(*[F.col(c) for c in group_cols]).agg(*agg_exprs)


def apply_calculation(df: DataFrame, params: dict) -> DataFrame:
    """
    Add calculated column(s) from expressions. Params: columns (dict name -> SQL expr) or name + expr.
    Alias for with_columns / with_column for clarity in config.
    """
    columns = params.get("columns")
    if columns:
        return apply_with_columns(df, {"columns": columns})
    name = params.get("name")
    expr = params.get("expr")
    if name and expr is not None:
        return df.withColumn(name, F.expr(expr))
    return df


# merge = union (alias)
def apply_merge(left: DataFrame, right: DataFrame, params: dict) -> DataFrame:
    """Merge (union) two DataFrames. Same as union with allow_missing_columns."""
    return apply_union(left, right, params)


# Transformation types that take two DataFrames (left, right) and return one
BINARY_TRANSFORMATION_TYPES = {"join", "union", "merge"}

# Registry: transformation type name -> (function, description)
TRANSFORMATION_REGISTRY: dict[str, tuple[callable, str]] = {
    "select": (apply_select, "Select columns"),
    "drop": (apply_drop, "Drop columns"),
    "filter": (apply_filter, "Filter rows by SQL expression"),
    "with_column": (apply_with_column, "Add/replace one column by expression"),
    "with_columns": (apply_with_columns, "Add/replace multiple columns"),
    "rename": (apply_rename, "Rename columns"),
    "sort": (apply_sort, "Order by columns"),
    "drop_duplicates": (apply_drop_duplicates, "Remove duplicate rows"),
    "limit": (apply_limit, "Limit number of rows"),
    "join": (apply_join, "Join two DataFrames (params: on, how)"),
    "union": (apply_union, "Union two DataFrames (params: allow_missing_columns)"),
    "merge": (apply_merge, "Merge (union) two DataFrames"),
    "date_format": (apply_date_format, "Format date/timestamp column(s)"),
    "aggregate": (apply_aggregate, "Group by and aggregate (avg, sum, min, max, count)"),
    "calculation": (apply_calculation, "Add calculated column(s) from expressions"),
}


def apply_transformation(df: DataFrame, step: dict) -> DataFrame:
    """
    Apply a single transformation step.
    step: {"type": "select", "params": {...}}
    """
    step_type = step.get("type")
    if not step_type:
        raise ValueError("Each transformation step must have a 'type' key")
    handler, _ = TRANSFORMATION_REGISTRY.get(step_type, (None, None))
    if handler is None:
        raise ValueError(
            f"Unknown transformation type: '{step_type}'. "
            f"Supported: {list(TRANSFORMATION_REGISTRY.keys())}"
        )
    params = step.get("params") or {}
    return handler(df, params)


def apply_all_transformations(df: DataFrame, transformations: list[dict]) -> DataFrame:
    """Apply a list of transformation steps in order (single-dataframe mode)."""
    for i, step in enumerate(transformations):
        df = apply_transformation(df, step)
    return df


def apply_binary_transformation(left: DataFrame, right: DataFrame, step: dict) -> DataFrame:
    """
    Apply a two-DataFrame step (join, union).
    step: {"type": "join"|"union", "params": {...}}
    """
    step_type = step.get("type")
    if step_type not in BINARY_TRANSFORMATION_TYPES:
        raise ValueError(f"Not a binary transformation: {step_type}. Use join or union.")
    handler, _ = TRANSFORMATION_REGISTRY[step_type]
    params = step.get("params") or {}
    return handler(left, right, params)
