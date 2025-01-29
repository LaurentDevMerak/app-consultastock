from sqlalchemy import create_engine
import urllib

def connect_to_database(database_path):
    """
    Establece una conexión a la base de datos Access utilizando SQLAlchemy.
    """
    try:
        # Crear la URL de conexión para SQLAlchemy
        connection_string = (
            "DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
            f"DBQ={database_path};"
        )
        connection_url = f"access+pyodbc:///?odbc_connect={urllib.parse.quote_plus(connection_string)}"
        engine = create_engine(connection_url)
        print(f"Conexión exitosa a la base de datos: {database_path}")
        return engine
    except Exception as e:
        print(f"Error al conectar con la base de datos {database_path}: {e}")
        return None

if __name__ == "__main__":
    # Ruta de prueba
    test_database_path = r"T:\Software DELSOL\FACTUSOL\Datos\FS\MES2025.accdb"

    print("Probando conexión a la base de datos...")
    engine = connect_to_database(test_database_path)

    if engine:
        print("Conexión establecida exitosamente.")
    else:
        print("No se pudo establecer la conexión.")
