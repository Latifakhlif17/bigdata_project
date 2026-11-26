#!/usr/bin/env bash
# Configuration commune à tous les scripts.
# Usage : export BUCKET=<ton_identifiant_onyxia> ; source src/config.sh

: "${BUCKET:?Définis d abord BUCKET, ex: export BUCKET=user-l-khlif-ece}"

# Client S3 = AWS CLI (lit seul les variables AWS_* injectées par Onyxia).
# Le stockage de l'école (SeaweedFS) demande des adresses « style chemin ».
command -v aws >/dev/null || { echo "AWS CLI absent : lance d'abord  pip install awscli" >&2; return 1 2>/dev/null || exit 1; }
aws configure set default.s3.addressing_style path

export PROJECT_PREFIX="bigdata_project"
export RAW_PREFIX="${PROJECT_PREFIX}/raw/sncf_regularite_tgv"
export PROCESSED_PREFIX="${PROJECT_PREFIX}/processed"
export RAW_FILE="regularite_mensuelle_tgv_liaisons.csv"

# Source : SNCF Voyageurs via data.gouv.fr (licence ODbL)
export DATA_URL="https://www.data.gouv.fr/api/1/datasets/r/91fe399d-cafa-4e72-8ba3-56d8717fdad4"

export LOCAL_DATA_DIR="data"          # copie locale de travail (non versionnée)
export S3_RAW="s3://${BUCKET}/${RAW_PREFIX}/${RAW_FILE}"
export S3_PROCESSED="s3://${BUCKET}/${PROCESSED_PREFIX}"
