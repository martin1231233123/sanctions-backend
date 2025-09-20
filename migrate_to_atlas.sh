#!/bin/bash
# ------------------------------------------------------
# 🚀 Migración de datos desde MongoDB Local (Docker) a Atlas
# ------------------------------------------------------

# 🔹 CONFIGURACIÓN
LOCAL_CONTAINER="mongodb-poc"                       # Nombre de tu contenedor local
LOCAL_DB="sanctions"                                # Base de datos a exportar
LOCAL_USER="admin"                                  # Usuario local
LOCAL_PASS="admin"                                  # Password local
BACKUP_DIR="$HOME/backup_sanctions"                 # Carpeta en tu host para el dump

# 🔹 Datos de Atlas
ATLAS_URI="mongodb+srv://martinrodriguezpra_db_user:kj3pRP4SJG5MdfTA@clustersanciones.inuhraz.mongodb.net/sanctions?retryWrites=true&w=majority"

# ------------------------------------------------------
echo "1️⃣  Creando carpeta de backup en $BACKUP_DIR ..."
mkdir -p "$BACKUP_DIR"

echo "2️⃣  Realizando dump de la base local ($LOCAL_DB) ..."
docker exec -it "$LOCAL_CONTAINER" mongodump \
  --username "$LOCAL_USER" \
  --password "$LOCAL_PASS" \
  --authenticationDatabase admin \
  --db "$LOCAL_DB" \
  --out /data/backup

echo "3️⃣  Copiando dump desde el contenedor a $BACKUP_DIR ..."
docker cp "$LOCAL_CONTAINER":/data/backup "$BACKUP_DIR"

echo "4️⃣  Instalando mongo-database-tools si no existen ..."
if ! command -v mongorestore &> /dev/null
then
    echo "⚠️  mongo-database-tools no encontrado. Instalando..."
    sudo apt update && sudo apt install -y mongodb-database-tools
fi

echo "5️⃣  Restaurando en MongoDB Atlas ..."
mongorestore --uri "$ATLAS_URI" \
  --dir "$BACKUP_DIR/backup/$LOCAL_DB" \
  --drop

echo "✅ Migración completada. Verifica en Atlas."

