from fastapi import FastAPI, Query, HTTPException
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from fastapi.responses import JSONResponse
import math
import os

app = FastAPI(title="Sanctions Lookup API", version="3.0")

# --- Leer URI de MongoDB desde variable de entorno ---
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError("❌ No se encontró la variable de entorno MONGO_URI. Configúrala en Render.")

collections = [
    "ofac_list",
    "un_list",
    "eu_list",
    "uk_list",
    "canada_list",
    "australia_list",
    "swiss_list",
    "worldbank_list",
    "iadb_list",
    "adb_list",
    "interpol_list",
]

def clean_doc(doc):
    """Limpia recursivamente NaN e Inf para que sea JSON serializable."""
    if isinstance(doc, dict):
        return {k: clean_doc(v) for k, v in doc.items()}
    elif isinstance(doc, list):
        return [clean_doc(v) for v in doc]
    elif isinstance(doc, float) and (math.isnan(doc) or math.isinf(doc)):
        return None
    else:
        return doc

# --- Conexión a MongoDB ---
try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    db = client["sanctions"]
    print("✅ Conexión a MongoDB exitosa")

    for coll in collections:
        db[coll].create_index([("name", "text")])
        print(f"📌 Índice de texto creado en {coll}")

except ServerSelectionTimeoutError as err:
    print("❌ No se pudo conectar a MongoDB:", err)
    db = None


@app.get("/")
def read_root():
    return {"message": "POC FastAPI + MongoDB OK 🚀"}


# ---------------------------------------------------------
# 🔎 Buscar persona por nombre y apellido en TODAS las listas
# ---------------------------------------------------------
@app.get("/search_person/")
def search_person(
    first_name: str = Query(..., description="Nombre a buscar"),
    surname: str = Query(..., description="Apellido a buscar")
):
    """
    Busca coincidencias de nombre y apellido en todas las listas MongoDB.
    Devuelve HTTP 200 si hay resultados, o HTTP 404 si no se encuentra nada.
    """
    if db is None:
        raise HTTPException(status_code=503, detail="MongoDB no está disponible")

    results = {}

    for coll in collections:
        try:
            # Buscamos coincidencias aproximadas (case-insensitive)
            query = {
                "$and": [
                    {"name": {"$regex": first_name, "$options": "i"}},
                    {"name": {"$regex": surname, "$options": "i"}}
                ]
            }
            cursor = db[coll].find(query, {"_id": 0})
            docs = [clean_doc(d) for d in cursor]
            if docs:
                results[coll] = docs
        except Exception as e:
            print(f"⚠️ Error buscando en {coll}: {e}")

    if not results:
        # 🔴 Si NO hay resultados, devolvemos 404 con mensaje
        raise HTTPException(status_code=404, detail="No se encontraron coincidencias")

    # ✅ Si hay resultados, devolvemos 200 con estructura estilo Factiva
    response = {
        "data": {
            "attributes": {
                "basic": {
                    "type": "Person",
                    "name_details": {
                        "primary_name": {
                            "first_name": first_name,
                            "surname": surname
                        }
                    }
                },
                "watchlist": {
                    "matches": results  # Aquí se listan todas las coincidencias por colección
                }
            }
        }
    }

    return JSONResponse(status_code=200, content=response)

