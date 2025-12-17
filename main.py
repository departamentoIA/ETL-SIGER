

from pkg.extraction import *


def main():
    """Ejecuta el pipeline E-T-L-EDA para todas las tablas."""
    print("--- INICIANDO PIPELINE ETL Y QA ---")

    for table_name in TABLES_TO_PROCESS:

        print("\n" + "=" * 25)
        print(f"| 📊 Procesando Tabla: {table_name}")
        print("=" * 25)

        try:
            # 1. Extracción (E)
            df = extract_from_file(table_name, ROOT_DATA_PATH)
            df_sample = df.sample(500)
            df_sample.write_excel(f'{table_name}_sample.xlsx')
            # 2. Transformación (T)
            # print(f"--- INICIANDO TRANSFORMACIÓN para {table_name} ---")
            # df = apply_transformation(table_name, df)
            # df_sample = df.sample(50)
            # df_sample.write_excel(f'{table_name}_trans.xlsx')
            # print("--- TRANSFORMACIÓN FINALIZADA ---")

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
