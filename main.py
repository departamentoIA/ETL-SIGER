

from pkg.extract import *
from pkg.transform import *


def main():
    """E-T-L pipeline."""
    for table_name in TABLES_TO_PROCESS:
        print("\n" + "=" * 25)
        print(f"| 📊 Procesando Tabla: {table_name}")
        print("=" * 25)
        try:
            # 1. Extraction (E)
            df = extract_from_file(table_name, ROOT_DATA_PATH)
            # 2. Transformation (T)
            df_trans = transform(df)
            df_sample = df_trans.sample(100, seed=42)
            try:
                df_sample.write_excel(f'{table_name}_trans.xlsx')
            except:
                print("\n\nNo puedo escribir en el excel si está abierto!")

            # 3. Carga (L) - L1 (Parquet) y L2 (SQL Server)
            # apply_loading(table_name, df)

            # 4. Análisis Exploratorio de Datos (EDA)
            # analyze_data_quality(df, table_name, REPORTS_DIR)

        except Exception as e:
            print(
                f"| ❌ FALLO CRÍTICO en el Pipeline para {table_name}. Mensaje:")
            print(f"'{e}'")
            print("=" * 55)

    print("\n--- PIPELINE COMPLETO FINALIZADO ---")


if __name__ == '__main__':
    main()
