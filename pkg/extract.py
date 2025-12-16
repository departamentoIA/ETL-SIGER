# extract.py
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


def sample_problematic_lines(file_path: Path, n_lines=10) -> None:
    """Muestra las primeras N líneas del archivo para inspección manual en caso de fallo."""
    print(
        f"\n--- MUESTRA DE LAS PRIMERAS {n_lines} LÍNEAS PARA INSPECCIÓN ({file_path.name}) ---")
    try:
        # Uso de'latin1'
        with open(file_path, 'rb') as f_bin:
            f = io.TextIOWrapper(f_bin, encoding='latin1', newline='')
            for i in range(n_lines):
                line = f.readline()
                if not line:
                    break
                print(f"[{i+1}]: {line.strip()[:150]}...")
    except Exception as e:
        print(f"Error al leer la muestra: {e}")
    print("----------------------------------------------------------------------\n")


def extract_from_file(table_name: str, root_path: Path, limit: Optional[int] = None) -> pl.DataFrame:
    """Función principal para dirigir la extracción robusta."""
    print(f"--- INICIANDO EXTRACCIÓN para {table_name} ---")
    file_path = get_file_paths(table_name, root_path)
    if not file_path:
        raise FileNotFoundError(
            f"No se encontró el archivo para '{table_name}'.")
    # --- 1. Determinación de Columnas a Leer
    columns_to_read: Optional[List[str]] = None
    columns_to_exclude = set(COLUMNS_TO_EXCLUDE.get(table_name, []))
    all_columns: List[str] = []

    # === LÓGICA DE CORRECCIÓN DELIMITADOR ===
    delimiter = ',' if file_path.suffix == '.csv' else '|'

    if table_name == 'MVCARATULAS':
        delimiter = '|'
    # =======================================
    if table_name in TABLES_REQUIRING_MANUAL_HEADER:
        try:
            with open(file_path, 'rb') as f_bin:
                f = io.TextIOWrapper(f_bin, encoding='latin1', newline='')
                header_line = f.readline().strip()

            raw_columns = [col.strip().strip('"')
                           for col in header_line.split(delimiter)]
            all_columns = [col for col in raw_columns if col]

            if not all_columns or len(all_columns) < 2:
                raise ValueError("Encabezado inválido o no encontrado.")

        except Exception as e:
            sample_problematic_lines(file_path)
            raise ValueError(f"Error al leer encabezado manualmente: {e}")

        columns_to_read = [
            col for col in all_columns if col not in columns_to_exclude]

        if columns_to_exclude and (table_name not in TABLES_MANUAL_CLEANUP):
            print(f"Excluyendo campos problemáticos: {columns_to_exclude}")

    # --- 2. Desvío para Limpieza Manual
    if table_name in TABLES_MANUAL_CLEANUP:
        return extract_with_manual_clean(table_name, file_path, limit, all_columns)

    # --- 3. Lectura Estándar de Polars para el resto de tablas ---
    print(f"Extrayendo datos de: {file_path.as_posix()}")

    read_params: Dict[str, Any] = {
        'separator': delimiter,
        'infer_schema_length': 100000,
        'schema_overrides': SCHEMA_OVERRIDES.get(table_name, {}),
        'n_rows': limit,
        'encoding': "latin1",
        'quote_char': '\"',
        'rechunk': True,
    }

    if table_name in TABLES_REQUIRING_MANUAL_HEADER:
        read_params.update({
            'has_header': False,
            'skip_rows': 1,
            'new_columns': columns_to_read,
            'ignore_errors': True,
        })
    else:
        read_params.update({
            'has_header': True,
            'ignore_errors': False,
        })

    try:
        df = pl.read_csv(file_path.as_posix(), **read_params)
    except Exception as e:
        sample_problematic_lines(file_path)
        raise e

    print(f"Datos extraídos: {df.shape[0]} filas, {df.shape[1]} columnas.")
    return df

# extract_with_manual_clean --------------------------------------------


def extract_with_manual_clean(table_name: str, file_path: Path, n_rows_limit: Optional[int] = None, all_columns: list = None) -> pl.DataFrame:
    '''Limpieza manual'''
    print(f"--- INICIANDO LIMPIEZA MANUAL Y EXTRACCIÓN para {table_name} ---")
    clean_lines = []
    anomaly_log = []
    columns_to_exclude = set(COLUMNS_TO_EXCLUDE.get(table_name, []))

    # Ruta de la carpeta 'anomalies' para los logs
    current_dir = Path(__file__).resolve().parent
    ANOMALIES_DIR = current_dir.parent / 'anomalies'

    # 1. Determinar el delimitador
    delimiter = '|'

    try:
        # --- LECTURA BINARIA ROBUSTA PARA EVITAR ERRORES DE ENCODING ---
        with open(file_path, 'rb') as f_bin:
            f = io.TextIOWrapper(f_bin, encoding='latin1', newline='')

            reader = csv.reader(f, delimiter=delimiter, quotechar='"')
            # --- FIN LECTURA ROBUSTA ---

            # 1. Procesar encabezado
            if not all_columns:
                header = next(reader)
                all_columns = [col.strip().strip('"') for col in header]
            else:
                try:
                    next(reader)
                except StopIteration:
                    pass

            columns_to_read = [
                col for col in all_columns if col not in columns_to_exclude]
            clean_lines.append(delimiter.join(columns_to_read))

            # 2. Iterar sobre las filas y aplicar limpieza
            for i, row in enumerate(reader):
                temp_row = list(row)
                expected_len = len(all_columns)

                delimiter_replace = ' '
                temp_row = [
                    val.replace('\n', ' ').replace('\r', ' ').replace(
                        '"', ' ').replace('|', delimiter_replace)
                    for val in temp_row
                ]
                # --- FIN LÓGICA DE LIMPIEZA DE CARACTERES ---

                if len(temp_row) > expected_len:
                    anomaly_log.append(
                        f"Registro truncado (CLOB desbordado): Linea {i+2} -> Obtenido {len(temp_row)}, Truncado a {expected_len}")
                    temp_row = temp_row[:expected_len]

                elif len(temp_row) < expected_len:
                    anomaly_log.append(
                        f"Registro saltado (Longitud Corta): Linea {i+2} -> Esperado {expected_len}, Obtenido {len(temp_row)}")
                    continue

                # 3. Excluir columnas de la fila antes de unir
                final_row = []
                for col_name, col_value in zip(all_columns, temp_row):
                    if col_name not in columns_to_exclude:
                        final_row.append(col_value)

                if len(final_row) != len(columns_to_read):
                    anomaly_log.append(
                        f"Registro saltado (Error Lógica Exclusión): Linea {i+2}")
                    continue

                clean_lines.append(delimiter.join(final_row))

                if isinstance(n_rows_limit, int) and i >= n_rows_limit:
                    break

    except Exception as e:
        print(f"Error fatal durante la lectura manual: {e}")
        sample_problematic_lines(file_path)
        raise

    # 4. Registrar anomalías
    if anomaly_log:
        log_file = ANOMALIES_DIR / \
            f"{table_name}_manual_anomalies_{datetime.now():%Y%m%d_%H%M%S}.csv"
        ANOMALIES_DIR.mkdir(exist_ok=True)
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write("Anomaly_Details\n")
            for log in anomaly_log:
                f.write(f"{log}\n")
        print(
            f"  {len(anomaly_log)} Anomalías registradas y saltadas/truncadas en: {log_file.name}")

    # 5. Cargar el texto limpio en Polars
    clean_data_str = "\n".join(clean_lines)

    df = pl.read_csv(
        io.StringIO(clean_data_str),
        separator=delimiter,
        has_header=True,
        schema_overrides=SCHEMA_OVERRIDES.get(table_name, {}),
        encoding="utf8",
        rechunk=True,
        quote_char='\"',
        ignore_errors=True
    )

    print(f"Datos extraídos: {df.shape[0]} filas, {df.shape[1]} columnas.")
    return df
