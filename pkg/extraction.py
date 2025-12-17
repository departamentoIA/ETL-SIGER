# extraction.py
"""This file calls 'globals.py'."""
from pkg.globals import *


def get_file_paths(table_name: str, root_path: Path) -> Optional[Path]:
    """Busca el archivo de datos (.csv o .txt) para la tabla dada."""
    file_path_csv = root_path / f"{table_name}.csv"
    file_path_txt = root_path / f"{table_name}.txt"
    if file_path_csv.exists():
        return file_path_csv
    if file_path_txt.exists():
        return file_path_txt
    return None


def extract_from_file(table_name: str, root_path: Path, limit: Optional[int] = None) -> pl.DataFrame:
    """Función principal para dirigir la extracción robusta."""
    print(f"--- INICIANDO EXTRACCIÓN para {table_name} ---")
    file_path = get_file_paths(table_name, root_path)
    if not file_path:
        raise FileNotFoundError(
            f"No se encontró el archivo para '{table_name}'.")

    if table_name in ['MVCARATULAS', 'MVSOLICITUDES']:
        delimiter = '|'
    if table_name in ['DTFIRMAS']:
        quote_char = ''
    else:
        quote_char = '"'

    df = pl.read_csv(
        file_path,
        separator=delimiter,
        quote_char=quote_char,
        has_header=True,
        encoding="utf8-lossy",      # evita errores por caracteres raros
        # dtypes=pl.Utf8,
        ignore_errors=True,         # útil si hay filas dañadas
        low_memory=True,            # reduce uso de RAM
    )
    print(f"DataFrame con {df.shape[0]} filas y {df.shape[1]} columnas")
    return df
