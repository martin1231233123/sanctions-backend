#!/usr/bin/env python3
# sanctions_loader_to_atlas.py
import os
import csv
import requests
import math
import time
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from xml.etree import ElementTree as ET
import pandas as pd

# --- Config ---
DATA_DIR = "./data"
# Lee la URI desde variable de entorno, si no existe usa el Mongo local (útil para desarrollo)
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://admin:admin@localhost:27017/?authSource=admin"
)
DB_NAME = "sanctions"

# --- Util: limpieza recursiva NaN/inf ---
def clean_value(v):
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, dict):
        return {k: clean_value(val) for k, val in v.items()}
    if isinstance(v, list):
        return [clean_value(x) for x in v]
    return v

def clean_doc(doc):
    return {k: clean_value(v) for k, v in doc.items()}

# --- Conexión a Mongo ---
def get_db():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
        client.admin.command("ping")
        db = client[DB_NAME]
        print(f"✅ Conectado a MongoDB: {MONGO_URI.split('@')[-1]}")
        return db
    except ServerSelectionTimeoutError as e:
        print("❌ No se pudo conectar a MongoDB:", e)
        return None

# --- Función genérica para CSV ---
def load_csv(db, key, url=None, local_file=None, name_col_index=1, program_col_start=2, delimiter=','):
    try:
        print(f"\n⏳ Procesando lista {key.upper()}...")
        collection = db[key]
        # Vaciar colección (cambio idempotente)
        collection.drop()
        print(f"🗑 Colección {key} vaciada")

        text = None
        if url:
            try:
                resp = requests.get(url, timeout=30)
                resp.raise_for_status()
                text = resp.text
                print(f"🌐 Lista {key} descargada desde URL")
            except Exception as e:
                print(f"⚠️ Error descargando {key} desde URL: {e}")

        if text is None and local_file and os.path.exists(local_file):
            with open(local_file, 'r', encoding='utf-8', errors='ignore') as f:
                text = f.read()
            print(f"📂 Lista {key} cargada desde {local_file}")

        if text is None:
            print(f"❌ No se encontró archivo/URL para {key}, se omite")
            return

        reader = csv.reader(text.splitlines(), delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
        header = next(reader, None)
        if header and len(header) > 0:
            header[0] = header[0].lstrip('\ufeff')

        docs = []
        count = 0
        for row in reader:
            if not row or len(row) <= name_col_index:
                continue
            name = row[name_col_index].strip()
            program = row[program_col_start:] if len(row) > program_col_start else []
            doc = {"name": name, "program": program, "raw": row}
            docs.append(clean_doc(doc))
            count += 1
            if count % 500 == 0:
                print(f"   🔹 {key}: {count} filas procesadas...")

        if docs:
            collection.insert_many(docs)
            print(f"✅ {key}: {len(docs)} registros cargados")
        else:
            print(f"⚠️ {key}: No se encontraron registros válidos")

    except Exception as e:
        print(f"❌ Error procesando {key}: {e}")

# --- Función para XML ---
def load_xml(db, key, url=None, local_file=None):
    try:
        print(f"\n⏳ Procesando lista {key.upper()} (XML)...")
        collection = db[key]
        collection.drop()
        print(f"🗑 Colección {key} vaciada")

        content = None
        if url:
            try:
                resp = requests.get(url, timeout=30)
                resp.raise_for_status()
                content = resp.content
                print(f"🌐 Lista {key} descargada desde URL")
            except Exception as e:
                print(f"⚠️ Error descargando {key} desde URL: {e}")

        if content is None and local_file and os.path.exists(local_file):
            with open(local_file, 'rb') as f:
                content = f.read()
            print(f"📂 Lista {key} cargada desde {local_file}")

        if content is None:
            print(f"❌ No se encontró archivo/URL para {key}, se omite")
            return

        root = ET.fromstring(content)
        docs = []
        count = 0

        entities = root.findall(".//INDIVIDUAL") + root.findall(".//ENTITY") + root.findall(".//RECORD")
        for entity in entities:
            name = (
                (entity.findtext("FIRST_NAME", "") or "") + " " +
                (entity.findtext("SECOND_NAME", "") or "") + " " +
                (entity.findtext("LAST_NAME", "") or "")
            ).strip() or entity.findtext("NAME", "").strip()
            if not name:
                continue
            raw_xml = ET.tostring(entity, encoding="unicode")
            doc = {"name": name, "raw": raw_xml}
            docs.append(clean_doc(doc))
            count += 1
            if count % 100 == 0:
                print(f"   🔹 {key}: {count} registros procesados...")

        if docs:
            collection.insert_many(docs)
            print(f"✅ {key}: {len(docs)} registros cargados")
        else:
            print(f"⚠️ {key}: No se encontraron registros válidos")

    except Exception as e:
        print(f"❌ Error procesando {key}: {e}")

# --- Función para Excel ---
def load_excel(db, key, local_file):
    try:
        print(f"\n⏳ Procesando lista {key.upper()} desde Excel...")
        if not os.path.exists(local_file):
            print(f"❌ Archivo {local_file} no encontrado")
            return

        collection = db[key]
        collection.drop()
        print(f"🗑 Colección {key} vaciada")

        df = pd.read_excel(local_file)
        docs = []
        for _, row in df.iterrows():
            name = str(row.iloc[0]).strip() if len(row) > 0 else ""
            program = list(row.iloc[1:]) if len(row) > 1 else []
            doc = {"name": name, "program": program, "raw": row.to_dict()}
            docs.append(clean_doc(doc))

        if docs:
            collection.insert_many(docs)
            print(f"✅ {key}: {len(docs)} registros cargados")
        else:
            print(f"⚠️ {key}: No se encontraron registros válidos")

    except Exception as e:
        print(f"❌ Error procesando {key}: {e}")

# --- MAIN ---
if __name__ == "__main__":
    print("🚀 Iniciando loader hacia MongoDB...")
    db = get_db()
    if db is None:
        print("❌ No hay conexión a MongoDB. Revisa MONGO_URI y acceso a red.")
        exit(1)

    # Ejemplos: ajusta rutas locales si hace falta
    load_csv(db, "ofac_list", url="https://www.treasury.gov/ofac/downloads/sdn.csv")
    load_xml(db, "un_list", url="https://scsanctions.un.org/resources/xml/en/consolidated.xml")
    load_csv(db, "eu_list", url="https://webgate.ec.europa.eu/europeaid/fsd/fsf/public/files/csvFullSanctionsList/content?token=dG9rZW4tMjAxNw")

    # Local files (ajusta DATA_DIR/nombres de archivos)
    load_csv(db, "uk_list", local_file=os.path.join(DATA_DIR, "UK_Sanctions_List.csv"))
    load_xml(db, "canada_list", local_file=os.path.join(DATA_DIR, "CANADA_sanctions.xml"))
    load_excel(db, "australia_list", local_file=os.path.join(DATA_DIR, "AU_sanctions.xlsx"))
    load_excel(db, "worldbank_list", local_file=os.path.join(DATA_DIR, "WB_sanctions.xlsx"))
    load_csv(db, "iadb_list", local_file=os.path.join(DATA_DIR, "IADB_sanctions.csv"))

    # Crear índices de texto en "name" donde sea relevante (opcional)
    print("\n🔎 Creando índices de texto donde existan colecciones...")
    for coll_name in db.list_collection_names():
        try:
            db[coll_name].create_index([("name", "text")])
            print(f"📌 Índice texto creado en {coll_name}")
        except Exception as e:
            print(f"⚠️ No pudo crear índice en {coll_name}: {e}")

    print("\n✅ Carga finalizada.")

