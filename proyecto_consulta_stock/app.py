from flask import Flask, render_template, request, jsonify
from concurrent.futures import ThreadPoolExecutor
from db_connector import connect_to_database
from query_handler import fetch_stock_data
import pandas as pd

app = Flask(__name__)

DATABASES = DATABASES = {
    "Komerco": r"Z:\Software DELSOL\FACTUSOL\Datos\FS\KOM2025.accdb",
    "Mesones": r"T:\Software DELSOL\FACTUSOL\Datos\FS\MES2025.accdb",
    "Argentina": r"X:\ARG2025.accdb",
    "Brasil": r"W:\FS\BRA2025.accdb",
    "Peña": r"V:\AYN2025.accdb",
    "Fray Servando": r"U:\FRA2025.accdb",
    "Venustiano": r"Y:\VCZ2025.accdb",
    "Puebla 5NTE": r"S:\Software DELSOL\FACTUSOL\Datos\FS\PUE2025.accdb",
    "Puebla 10PTE": r"C:\Users\Admin\Komerco Hodiau\Sucursales Gallos BI - Factusol\P10\PTE2025.accdb"
}

# Caché para almacenar resultados de consultas previas
cache = {}

@app.route("/")

def home():
    return "¡Hola desde PythonAnywhere!"

def index():
    return render_template("index.html")

@app.route("/consultar", methods=["POST"])
def consultar():
    code = request.form.get("codigo")
    if not code:
        return jsonify({"error": "Por favor ingresa un código de producto."}), 400

    # Revisar si la consulta ya está en caché
    if code in cache:
        print(f"Resultado para '{code}' obtenido de la caché.")
        return jsonify(cache[code])  # Retornar resultados desde la caché

    # Consultar las bases de datos en paralelo
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(fetch_data_from_db, store_name, db_path, code)
            for store_name, db_path in DATABASES.items()
        ]
        results = [future.result() for future in futures if not future.result().empty]

    if not results:
        return jsonify({"error": f"No se encontraron resultados para el código '{code}'"}), 404

    # Combinar resultados en un solo DataFrame
    final_df = pd.concat(results, ignore_index=True)

    # Convertir los datos a JSON
    data = final_df.to_dict(orient="records")

    # Guardar los resultados en la caché
    cache[code] = data
    print(f"Resultado para '{code}' guardado en la caché.")

    return jsonify(data)

def fetch_data_from_db(store_name, db_path, code):
    """
    Función para consultar una base de datos específica.
    """
    engine = connect_to_database(db_path)
    if not engine:
        return pd.DataFrame()

    try:
        with engine.connect() as conn:
            filters = {"Código": code}
            df = fetch_stock_data(conn, filters)
            if not df.empty:
                df["Sucursal"] = store_name
            return df
    except Exception as e:
        print(f"Error al consultar la base de datos {store_name}: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)