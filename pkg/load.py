# load.py
"""This file calls 'globals.py' and 'config.py'."""
from pkg.globals import *
from sqlalchemy import create_engine
from pkg.config import get_connection_string


def load_table(df: pl.DataFrame, table_name: str) -> None:
    """Load the DataFrame to SQL Server by using SQLAlchemy."""
    # 'fast_executemany=True' is the secret to speed in SQL Server
    engine = create_engine(get_connection_string(), fast_executemany=True)
    print(f"Subiendo tabla '{table_name}' a SQL Server...")
    create_table = "replace"
    try:
        df.write_database(
            table_name=table_name,
            connection=engine,
            if_table_exists=create_table
        )

    except Exception as e:
        print(f"❌ Error: {e}")
