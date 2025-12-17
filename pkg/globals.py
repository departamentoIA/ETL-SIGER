# globals.py
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

ROOT_DATA_PATH = Path(
    r"D:\caarteaga\Documents\Departamento de IA\Trabajos\SIGER Documentos\entrega_siger")

TABLES_TO_PROCESS: List[str] = [
    'MVSOLICITUDES',
    'MVCARATULAS'
]
