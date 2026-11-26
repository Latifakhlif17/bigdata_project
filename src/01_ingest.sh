#!/usr/bin/env bash
# Étape 1 — Téléchargement des données brutes et dépôt sur le stockage S3 d'Onyxia.
# Idempotent : relancer le script réécrit simplement le même fichier.
set -euo pipefail
cd "$(dirname "$0")/.."
source src/config.sh
bash src/whoami.sh

mkdir -p "${LOCAL_DATA_DIR}/raw"
LOCAL="${LOCAL_DATA_DIR}/raw/${RAW_FILE}"

echo "[1/4] Téléchargement depuis ${DATA_URL}"
curl -L --fail --retry 3 -o "${LOCAL}" "${DATA_URL}"

echo "[2/4] Contrôles du fichier brut"
SIZE=$(stat -c%s "${LOCAL}")
LINES=$(wc -l < "${LOCAL}")
NCOLS=$(head -1 "${LOCAL}" | awk -F';' '{print NF}')
SHA=$(sha256sum "${LOCAL}" | cut -d' ' -f1)
echo "    taille = ${SIZE} octets | lignes = ${LINES} | colonnes = ${NCOLS}"
if [ "${SIZE}" -lt 1000000 ] || [ "${SIZE}" -gt 5000000 ]; then
  echo "    ATTENTION : taille hors de l'intervalle 1–5 Mo imposé par le sujet !" >&2
  exit 1
fi
if [ "${NCOLS}" -ne 26 ]; then
  echo "    ATTENTION : 26 colonnes attendues, ${NCOLS} trouvées (le format source a changé ?)" >&2
  exit 1
fi

cat > "${LOCAL_DATA_DIR}/raw/_metadata.json" <<EOF
{
  "source_url": "${DATA_URL}",
  "producteur": "SNCF Voyageurs (via data.gouv.fr)",
  "licence": "ODbL",
  "date_telechargement": "$(date -u '+%Y-%m-%dT%H:%M:%SZ')",
  "taille_octets": ${SIZE},
  "nb_lignes_avec_entete": ${LINES},
  "sha256": "${SHA}"
}
EOF

echo "[3/4] Envoi sur S3 : s3://${BUCKET}/${RAW_PREFIX}/"
aws s3 cp "${LOCAL}" "s3://${BUCKET}/${RAW_PREFIX}/${RAW_FILE}"
aws s3 cp "${LOCAL_DATA_DIR}/raw/_metadata.json" "s3://${BUCKET}/${RAW_PREFIX}/_metadata.json"

echo "[4/4] Contenu du bucket"
aws s3 ls --recursive --human-readable "s3://${BUCKET}/${PROJECT_PREFIX}/"
