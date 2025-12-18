# transform.py
"""This file calls 'globals.py'."""
from pkg.globals import *
from typing import Iterable


def drop_columns(df: pl.DataFrame, cols: List[str]) -> pl.DataFrame:
    """Drop some columns."""
    colums = [c for c in cols if c in df.columns]
    return df.drop(colums)


def cast_columns(df: pl.DataFrame, columns: Iterable[str], dtype: pl.DataType
                 ) -> pl.DataFrame:
    """Cast the specified columns to the specified type.
    Only apply the cast if the column exists."""
    return df.with_columns(
        [
            pl.col(col).cast(dtype)
            for col in columns
            if col in df.columns
        ]
    )


def parse_datetime_columns(df: pl.DataFrame, columns: Iterable[str],
                           fmt: str = "%Y-%m-%d %H:%M:%S", strict: bool = False) -> pl.DataFrame:
    """Converts text columns (Utf8) to pl.Datetime using strptime.
    The conversion only applies if the column exists and is Utf8."""
    return df.with_columns(
        [
            pl.col(col)
              .str.strptime(pl.Datetime, format=fmt, strict=strict)
              .alias(col)
            for col in columns
            if col in df.columns and df.schema[col] == pl.Utf8
        ]
    )


def to_uppercase_col(df: pl.DataFrame, columns: Iterable[str]) -> pl.DataFrame:
    """Convert to uppercase."""
    return df.with_columns(
        [
            pl.col(col).str.strip_chars().str.to_uppercase().alias(col)
            for col in columns
            if col in df.columns
        ]
    )


def transform(df: pl.DataFrame) -> pl.DataFrame:
    """Drop some columns, apply cast and formating to the DataFrames."""
    df = drop_columns(df, col_drop)
    df = cast_columns(df, col_int32, pl.Int32)
    df = cast_columns(df, col_int8, pl.Int8)
    df = parse_datetime_columns(df, col_date)
    df = to_uppercase_col(df, col_uppercase)
    print(f"DataFrame con {df.shape[0]} filas y {df.shape[1]} columnas")
    print(df.schema)
    return df
