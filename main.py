from fastapi import FastAPI, Query
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
import math
import os

app = FastAPI(title="Sanctions Lookup API", version="2.3")

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
# 🔎 1. Buscar persona o empresa en TODAS las listas
# ---------------------------------------------------------
@app.get("/search_all_lists/")
def search_all_lists(
    person: str | None = Query(None, description="Nombre de la persona a buscar"),
    company: str | None = Query(None, description="Nombre de la empresa a buscar")
):
    """
    Recorre todas las listas y devuelve coincidencias para
    - persona (campo 'name')
    - empresa (campo 'company' o similar)
    """
    if db is None:
        return {"error": "MongoDB no está disponible"}

    if not person and not company:
        return {"error": "Debes especificar al menos 'person' o 'company'"}

    results = {}

    for coll in collections:
        query = {"$or": []}
        if person:
            query["$or"].append({"name": {"$regex": person, "$options": "i"}})
        if company:
            # suponiendo que la colección tenga un campo 'company' o similar
            query["$or"].append({"company": {"$regex": company, "$options": "i"}})

        if not query["$or"]:
            continue

        try:
            cursor = db[coll].find(query, {"_id": 0})
            docs = [clean_doc(d) for d in cursor]
            if docs:
                results[coll] = docs
        except Exception as e:
            print(f"⚠️ Error buscando en {coll}: {e}")
            results[coll] = []

    return {
        "person": person,
        "company": company,
        "results": results,
        "message": None if results else "No se encontraron coincidencias"
    }

# ---------------------------------------------------------
# 🏢 2. Buscar a qué persona(s) pertenece una empresa
# ---------------------------------------------------------
@app.get("/company_owners/")
def company_owners(
    company: str = Query(..., description="Nombre de la empresa para encontrar sus dueños")
):
    """
    Busca el nombre de la empresa en todas las listas
    y devuelve los registros que incluyan persona(s) vinculadas.
    """
    if db is None:
        return {"error": "MongoDB no está disponible"}

    results = {}

    for coll in collections:
        try:
            # Buscamos coincidencias de empresa en campo 'company'
            cursor = db[coll].find({"company": {"$regex": company, "$options": "i"}}, {"_id": 0})
            docs = [clean_doc(d) for d in cursor]
            if docs:
                results[coll] = docs
        except Exception as e:
            print(f"⚠️ Error buscando en {coll}: {e}")
            results[coll] = []

    return {
        "company": company,
        "results": results,
        "message": None if results else "No se encontraron coincidencias"
    }

# ---------------------------------------------------------
# ✅ Endpoints originales (se mantienen para compatibilidad)
# ---------------------------------------------------------
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
            print(f"⚠️ Error en {coll}: {e}")
            results[coll] = []
    return {"query": name, "results": results,
            "message": None if any(results.values()) else "No se encontraron coincidencias"}

