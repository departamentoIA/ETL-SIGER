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
    'MVSOLICITUDES',
    # 'MVCARATULAS', 'CTSOCIOS', 'CFVARIABLES', 'MVVARACTO', 'MVFRMACTO', 'CFVARIABLES',
    # 'MVSOLICITUDES', 'BTSOLICITUDES', 'CTUSUARIOS_202511281728'
]

# Tables with ',' as delimiter
delimiter_comma = []

# Tables with '"' inside the cells
quote_char_double_quotes = ['MVCARATULAS', 'MVFRMACTO', 'MVSOLICITUDES',
                            'CTUSUARIOS_202511281728']

# Columns to be dropped
col_drop = ['CRFME', 'DSANTREG', 'FCCONSTITUCION', 'CFPAGODERECHOS_LLOFICINA',
            'FCNACIMIENTO', 'FCMODIFICACION', 'DSDOMICILIO', 'NOMONTO',
            'LLESTDOACTOPRE', 'DSACTO', 'NOCONSECUTIVO', 'DSNOMBRERESPOFIC',
            'LLMVFRMACTOSUB', 'FCINGRESO', 'FCINSCRIPCION', 'LLGRUPOTRABAJO',
            ]

# Columns 'Int32' type for all tables
col_int32 = ['LLCARATULA', 'LLOFICINA', 'LLMUNICIPIO', 'LLSOCIO', 'LLESTADO',
             'LLGIRO', 'LLTIPOSOCIEDAD', 'LLVARIABLE', 'LLNACIONALIDAD',
             'LLESTATUSCARATULA', 'LLPAGODERECHO', 'LLMVFRMACTO', 'LLSOLFRMPRE',
             'LLESTDOACTO', 'LLFORMAACTO', 'LLVARACTO', 'LLTIPOSOLICITUD',
             'LLETAPA', 'LLENTREGA', 'LLCOMPFEDA', 'LLUSUARIO',
             ]

# Columns 'Int8' type for all tables
col_int8 = ['BOMORAL', 'BOACERVO', 'BOACTIVO', 'BOVARIABLERANGO',
            'BOANOTACIONACTIVA']

# Columns 'DATE' type for all tables
col_date = ['FCAPERTURA', 'FCNACIMIENTO', 'FCCONSTITUCION', 'FCMODIFICACION',
            'FCINGRESO', 'FCINSCRIPCION']

# String Columns to be converted to uppercase
col_uppercase = ['DSDENSOCIAL', 'DSDIRECCION', 'DSDURACION', 'DSNOMBRESOCIO',
                 'DSGIRO', 'DSTIPOSOCIEDAD', 'DSOBJETO', 'DSAPELLIDOPATERNO',
                 'DSNACIONALIDAD', 'DSAPELLIDOMATERNO', 'DSRFC', 'DSCURP',
                 'DSNOMBREVARIABLE', 'DSDESCRIPCION', 'DSCODIGO']
