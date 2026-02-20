# load.py
"""This file calls 'globals.py' and 'config.py'."""
from pkg.globals import *
from sqlalchemy import create_engine, text
from pkg.config import get_connection_string


def map_polars_to_sql(colname: str, dtype: pl.DataType):
    """Get the SQL type of every column."""
    if dtype == pl.Utf8 and colname in LONG_TEXT_COLS:
        return "NVARCHAR(MAX)"

    mapping = {
        pl.Int8: "TINYINT",
        pl.Int16: "SMALLINT",
        pl.Int32: "INT",
        pl.Int64: "BIGINT",
        pl.Boolean: "BIT",
        pl.Float32: "REAL",
        pl.Float64: "FLOAT",
        pl.Utf8: "NVARCHAR(255)",
        pl.Date: "DATE",
        pl.Datetime: "DATETIME2"
    }
    return mapping.get(dtype, "NVARCHAR(255)")


def create_table_from_df(engine, table_name: str, df: pl.DataFrame,
                         primary_key: str | None = None) -> None:
    """Create table in SQL Server by using both SQL commands and DataFrame scheme."""
    full_name_for_object_id = f"dbo.{table_name}"
    full_name_bracket = f"[dbo].[{table_name}]"
    columns_sql = []

    for col, dtype in df.schema.items():
        sql_type = map_polars_to_sql(col, dtype)
        columns_sql.append(f"[{col}] {sql_type}")

    pk_sql = ""
    if primary_key:
        pk_sql = f", CONSTRAINT PK_{table_name} PRIMARY KEY ({primary_key})"

    create_sql = f"""
    IF OBJECT_ID(N'{full_name_for_object_id}', 'U') IS NOT NULL
        DROP TABLE {full_name_bracket};

    CREATE TABLE {full_name_bracket} (
        {', '.join(columns_sql)}
        {pk_sql}
    );
    """

    with engine.begin() as conn:
        conn.execute(text(create_sql))


def load_table(df: pl.DataFrame, table_name: str,
               batch_rows: int = 100_000    # It should be lower than 200k
               ) -> None:
    """Load the DataFrame to SQL Server by using SQLAlchemy."""
    # 'fast_executemany=True' is the secret to speed in SQL Server
    engine = create_engine(
        get_connection_string(),
        fast_executemany=True,
        pool_pre_ping=True,
        pool_recycle=1800,
    )
    full_name_bracket = f"dbo.{table_name}"
    create_table_from_df(engine, table_name, df, primary_keys.get(table_name))
    print(
        f"Subiendo tabla '{table_name}' a SQL Server en lotes de {batch_rows:,} filas...")
    # Batch insertion (pure Polars)
    n = df.height
    step = 0
    try:
        for start in range(0, n, batch_rows):
            chunk = df.slice(start, batch_rows)
            # In chunks always 'append'
            chunk.write_database(
                table_name=full_name_bracket,
                connection=engine,
                if_table_exists="append",
            )
            step += 1
            if step == 5:
                print(
                    f"✅ Filas completadas: {min(start + batch_rows, n):,} / {n:,}")
                step = 0

    except Exception as e:
        print(f"❌ Error cargando '{table_name}': {e}")
        raise
