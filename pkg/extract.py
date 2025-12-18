# extract.py
"""This file calls 'globals.py'."""
from pkg.globals import *


def get_file_paths(table_name: str, root_path: Path) -> Optional[Path]:
    """Obtain the full path and file extension."""
    file_path_csv = root_path / f"{table_name}.csv"
    file_path_txt = root_path / f"{table_name}.txt"
    if file_path_csv.exists():
        return file_path_csv
    if file_path_txt.exists():
        return file_path_txt
    return None


def extract_from_file(table_name: str, root_path: Path) -> pl.DataFrame:
    """Read files and construct DataFrames."""
    file_path = get_file_paths(table_name, root_path)
    if not file_path:
        raise FileNotFoundError(
            f"No se encontró el archivo para '{table_name}'.")

    delimiter = ',' if table_name in delimiter_comma else '|'
    quote_char = '"' if table_name in quote_char_double_quotes else None

    df = pl.read_csv(
        file_path,
        separator=delimiter,
        quote_char=quote_char,
        has_header=True,
        encoding="utf8-lossy",      # evita errores por caracteres raros
        dtypes=None,
        ignore_errors=True,         # útil si hay filas dañadas
        low_memory=True,            # reduce uso de RAM
    )
    # print(df.schema)
    print(f"DataFrame con {df.shape[0]} filas y {df.shape[1]} columnas")
    return df
