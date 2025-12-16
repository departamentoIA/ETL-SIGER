
import sys
import os
from pathlib import Path
import polars as pl
from typing import Dict, List
import time
import datetime
import polars as pl
from pathlib import Path
from datetime import datetime
import csv
import io
import re
from typing import Dict, List, Optional, Any

# RUTA ABSOLUTA DE LOS DATOS FUENTE (Unidad de red Z:)
ROOT_DATA_PATH = Path(
    r"D:\caarteaga\Documents\Departamento de IA\Trabajos\SIGER Documentos\entrega_siger")

# Lista de tablas a procesar
TABLES_TO_PROCESS: List[str] = [
    'MVCARATULAS'
    # 'MVSOLICITUDES', 'MVFRMACTO', 'CTSOCIOS',
    # 'MVVARACTO', 'MVDOCADJUNTOS', 'CFVARIABLES', 'CTUSUARIOS',
    # 'PAGO_PORTAL', 'CTGIROS', 'CTOFICINAS', 'CTTIPOSOCIEDAD'
]

# Tablas que requieren manejo manual
TABLES_MANUAL_CLEANUP = ['MVCARATULAS', 'CTSOCIOS', 'DTFIRMAS']

# Columnas excluidas
COLUMNS_TO_EXCLUDE = {
    'MVCARATULAS': ['DSOBJETO', 'DSDIRECCION'],
    'DTFIRMAS': ['DSCADORIGINAL', 'DSFIRMA'],
    'CTSOCIOS': [],
}


# Tablas que requieren manejo manual del encabezado
TABLES_REQUIRING_MANUAL_HEADER = [
    'MVSOLICITUDES', 'MVCARATULAS', 'CTSOCIOS', 'MVFRMACTO',
    'DTFIRMAS', 'MVDOCADJUNTOS', 'PAGO_PORTAL', 'CTUSUARIOS'
]

# Definición de tipos de datos forzados
SCHEMA_OVERRIDES: Dict[str, Dict[str, pl.DataType]] = {
    'MVSOLICITUDES': {'DSNCI': pl.Utf8},
    'PAGO_PORTAL': {'NOCONFIRMACIONRS': pl.Utf8, 'NOREFERENCIARS': pl.Utf8},
    'MVVARACTO': {'NOVALOR': pl.Float64},

    'CTSOCIOS': {
        'NOVALOR': pl.Float64,
        'LLRFC': pl.Utf8,
        'NOTOTAL': pl.Float64,
    },

    # MVCARATULAS: Forzado de tipos para las columnas restantes
    'MVCARATULAS': {
        'LLCARATULA': pl.Int64, 'LLOFICINA': pl.Int64, 'CRFME': pl.Utf8, 'FCAPERTURA': pl.Utf8, 'DSRFC': pl.Utf8,
        'DSANTREG': pl.Utf8, 'DSDENSOCIAL': pl.Utf8, 'LLMUNICIPIO': pl.Int64, 'DSMUNICIPIO': pl.Utf8,
        'LLESTADO': pl.Int64, 'DSESTADO': pl.Utf8, 'DSDURACION': pl.Utf8, 'LLGIRO': pl.Int64, 'DSGIRO': pl.Utf8,
        'LLTIPOSOCIEDAD': pl.Int64, 'DSDTIPOSOCIEDAD': pl.Utf8, 'LLNACIONALIDAD': pl.Int64, 'DSNACIONALIDAD': pl.Utf8,
        'DSCURP': pl.Utf8, 'BOMORAL': pl.Utf8, 'LLESTATUSCARATULA': pl.Int64, 'BOACERV': pl.Utf8,
    },

    'DTFIRMAS': {
        'LLFIRMA': pl.Int64, 'LLMVFRMACTO': pl.Int64, 'LLUSUARIO': pl.Int64,
        'DSHASH': pl.Utf8, 'FCFECHAFIRMA': pl.Utf8, 'LLETAPA': pl.Int64, 'NOSECUENCIA': pl.Int64,
    }
}


current_dir = Path(__file__).resolve().parent
sys.path.append(current_dir.as_posix())

# Directorios de salida
BASE_DIR = current_dir.parent
REPORTS_DIR = BASE_DIR / 'data' / 'reports'
ANOMALIES_DIR = BASE_DIR / 'anomalies'

# Asegurar que los directorios existan
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
ANOMALIES_DIR.mkdir(parents=True, exist_ok=True)
(BASE_DIR / 'data' / 'clean_data').mkdir(parents=True,
                                         exist_ok=True)  # Asegurar carpeta de Parquet

# Aumentar el límite del campo CSV para manejar CLOBs grandes (10 MB).
MAX_FIELD_SIZE = 10 * 1024 * 1024
try:
    csv.field_size_limit(MAX_FIELD_SIZE)
except OverflowError:
    csv.field_size_limit(sys.maxsize)
