# transform.py
"""This file calls 'globals.py'."""
from pkg.globals import *
from typing import Iterable
import datetime


def cast_columns(
    df: pl.DataFrame,
    columns: Iterable[str],
    dtype: pl.DataType,
) -> pl.DataFrame:
    """
    Castea las columnas indicadas al tipo especificado.
    Solo aplica el cast si la columna existe.
    """
    return df.with_columns(
        [
            pl.col(col).cast(dtype)
            for col in columns
            if col in df.columns
        ]
    )


def parse_datetime_columns(
    df: pl.DataFrame,
    columns: Iterable[str],
    fmt: str = "%Y-%m-%d %H:%M:%S%.f",
    strict: bool = False,
) -> pl.DataFrame:
    """
    Convierte columnas de texto (Utf8) a pl.Datetime usando strptime.
    Solo aplica la conversión si la columna existe y es Utf8.
    """
    return df.with_columns(
        [
            pl.col(col)
              .str.strptime(pl.Datetime, format=fmt, strict=strict)
              .alias(col)
            for col in columns
            if col in df.columns and df.schema[col] == pl.Utf8
        ]
    )


def transform(df: pl.DataFrame) -> pl.DataFrame:
    """Apply cast and formating to the DataFrames."""
    df = cast_columns(df, col_int32, pl.Int32)
    df = cast_columns(df, col_int8, pl.Int8)
    df = parse_datetime_columns(df, col_date)
    print(df.schema)
    return df
