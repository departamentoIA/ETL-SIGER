#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File:           main.py
Author:         Antonio Arteaga
Last Updated:   2025-02-03
Version:        1.0
Description:    DataFrames are obtained from CSV files, they are cleaned and 
                transformed, finally, all DataFrames are loaded to SQL Server (ETL).
Dependencies:   polars==1.38.1, openpyxl==3.1.5, xlsxwriter==3.2.9, pyarrow==23.0.1,
Usage:          CSV files are requested to run this script.
"""

from pkg.extract import *
from pkg.transform import *
from pkg.load import *

engine = create_engine(
    # 'fast_executemany=True' is the secret to speed in SQL Server
    get_connection_string(),
    fast_executemany=True,
    pool_pre_ping=True,
    pool_recycle=1800,
)


def write_df_sample(df: pl.DataFrame, table_name: str, text: str, n_rows: int) -> None:
    """Write an Excel file of n sample rows of the full DataFrame."""
    df_sample = df.sample(n_rows, seed=42)
    try:
        df_sample.write_excel(f'{table_name}_{text}.xlsx')
    except:
        print("\n\nNo puedo escribir en el excel si está abierto!")


def main():
    """E-T-L process."""
    for table_name in TABLES_TO_PROCESS:
        print("\n" + "=" * 25)
        print(f"📊 Procesando tabla: {table_name}")
        print("=" * 25)
        try:
            # 1. Extraction (E)
            df = extract_from_file(table_name, ROOT_PATH)
            # write_df_sample(df, table_name, "raw", 10)

            # 2. Transformation (T)
            df_trans = transform(df, table_name)
            # write_df_sample(df_trans, table_name, "clean", 10)

            # 3. Load to SQL Server (L) and create primary key
            load_table(engine, df_trans, table_name)

            # 4. Create table index
            create_index(engine, table_name, table_indexes)

        except Exception as e:
            print(
                f"\n❌ FALLO CRÍTICO para {table_name}.\n")
            print(f"'{e}'")
            print("=" * 25)

    print("\n--- PROCESO FINALIZADO ---")


if __name__ == '__main__':
    main()
