# globals.py
from pathlib import Path
import polars as pl
from typing import Dict, List
import polars as pl
from pathlib import Path
from typing import Dict, List, Optional, Any

ROOT_DATA_PATH = Path(
    r"D:\caarteaga\Documents\Departamento de IA\Trabajos\SIGER Documentos\entrega_siger")

TABLES_TO_PROCESS: List[str] = [
    'MVCARATULAS',
    # 'MVCARATULAS', 'CTSOCIOS', 'MVFRMACTO',
    # 'MVSOLICITUDES', 'BTSOLICITUDES', 'CTUSUARIOS_202511281728'
]

# Tables with ',' as delimiter
delimiter_comma = []

# Tables with '"' inside the cells
quote_char_double_quotes = ['MVCARATULAS', 'MVFRMACTO',
                            'MVSOLICITUDES', 'CTUSUARIOS_202511281728']

# Columns to be dropped
col_drop = []

# Columns 'Int32' type for all tables
col_int32 = ['LLCARATULA', 'LLOFICINA', 'LLMUNICIPIO',
             'LLESTADO', 'LLGIRO', 'LLTIPOSOCIEDAD',
             'LLNACIONALIDAD', 'LLESTATUSCARATULA'
             ]

# Columns 'Int8' type for all tables
col_int8 = ['BOMORAL', 'BOACERVO']

# Columns 'DATE' type for all tables
col_date = ['FCAPERTURA']
