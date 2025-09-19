from fastapi import FastAPI, Query
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
import math

app = FastAPI(title="Sanctions Lookup API", version="2.2")

MONGO_URI = "mongodb://admin:admin@localhost:27017/?authSource=admin"

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
    """
    Limpia recursivamente NaN e Inf para que sea JSON serializable.
    """
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

@app.get("/search/")
def search_sanctioned(name: str = Query(..., description="Nombre o empresa a buscar")):
    if db is None:
        return {"error": "MongoDB no está disponible"}

    results = {}
    search_filter = {"$text": {"$search": name}}

    for coll in collections:
        try:
            cursor = db[coll].find(search_filter, {"_id": 0})
            docs = [clean_doc(d) for d in cursor]
            results[coll] = docs
        except Exception as e:
            print(f"⚠️ Error buscando en {coll}: {e}")
            results[coll] = []

    return {
        "query": name,
        "results": results,
        "message": None if any(results.values()) else "No se encontraron coincidencias"
    }

@app.get("/search_exact/")
def search_exact(name: str = Query(..., description="Nombre exacto o regex")):
    if db is None:
        return {"error": "MongoDB no está disponible"}

    results = {}
    search_filter = {"name": {"$regex": f"^{name}$", "$options": "i"}}

    for coll in collections:
        try:
            cursor = db[coll].find(search_filter, {"_id": 0})
            docs = [clean_doc(d) for d in cursor]
            results[coll] = docs
        except Exception as e:
            print(f"⚠️ Error buscando en {coll}: {e}")
            results[coll] = []

    return {
        "query": name,
        "results": results,
        "message": None if any(results.values()) else "No se encontraron coincidencias"
    }

@app.get("/search_all/")
def search_all(name: str = Query(..., description="Nombre o empresa a buscar (case-insensitive)")):
    if db is None:
        return {"error": "MongoDB no está disponible"}

    results = {}
    search_filter = {"name": {"$regex": f"{name}", "$options": "i"}}

    for coll in collections:
        try:
            cursor = db[coll].find(search_filter, {"_id": 0})
            docs = [clean_doc(d) for d in cursor]
            results[coll] = docs
        except Exception as e:
            print(f"⚠️ Error buscando en {coll}: {e}")
            results[coll] = []

    return {
        "query": name,
        "results": results,
        "message": None if any(results.values()) else "No se encontraron coincidencias"
    }

