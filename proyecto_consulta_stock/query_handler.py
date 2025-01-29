import pandas as pd


def fetch_stock_data(engine, filters=None):
    #Ejecución de consulta SQL

    query = """
       SELECT 
            F_STO.ARTSTO AS Código, 
            SUM(F_STO.ACTSTO) AS Pzs, 
            IIF(F_ART.UPPART = 0 OR F_ART.UPPART IS NULL, 0, 
                ROUND(SUM(F_STO.ACTSTO) / F_ART.UPPART)) AS Cajas
        FROM 
            F_STO
        INNER JOIN 
            F_ART ON F_STO.ARTSTO = F_ART.CODART
        GROUP BY 
            F_STO.ARTSTO, F_ART.UPPART
    """

    if filters:
        # Cambiar claves del filtro a las columnas reales (no alias)
        column_map = {"Código": "Código", "Pzs": "Pzs", "Cajas":"Cajas"}
        additional_conditions = " AND ".join(
            [f"{column_map.get(key, key)} LIKE '%{value}%'" for key, value in filters.items()]
        )
        query = f"SELECT * FROM ({query}) AS SubQuery WHERE {additional_conditions}"

    try:
        print(f"Ejecutando consulta:\n{query}")
        #Ejecutamos la consulta y cargamos los datos en una DataFrame
        df = pd.read_sql(query, engine)
        if df.empty:
            print("Consulta ejecutada, pero no se encontraron resultados.")
        return df
    except Exception as e:
        print(f"Error al ejecutar la consulta: {e}")
        return pd.DataFrame()
    #Devuelve el Data Frame vacio en caso de Error


if __name__ == "__main__":
    from db_connector import connect_to_database
        # Ruta de la base de datos
    database_path = r"T:\Software DELSOL\FACTUSOL\Datos\FS\MES2025.accdb"

    engine = connect_to_database(database_path)

    if engine:
        print("Consultando datos...")

        #Prueba sin filtros
        df = fetch_stock_data(engine)
        print("Consulta sin filtros:")
        print(df)

        #Prueba con filtros
        filters = {"Código": "LV227"}
        df_filtered = fetch_stock_data(engine, filters)
        print("Consulta con filtros:")
        print(df_filtered)

    else:
        print("No se pudo establecer la conexión.")


