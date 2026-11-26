# Ponctualité des liaisons TGV — pipeline Big Data sur Onyxia

**Auteurs :** NOM1 Prénom1 — NOM2 Prénom2 (Groupe GrXX)

## Prérequis
- Compte Onyxia (instance ECE / Adaltas) ; service **VSCode-pyspark** (Python, Spark, identifiants S3 injectés en variables `AWS_*`).
- Client S3 : AWS CLI (`pip install awscli`), le client `mc` n'étant plus distribué par MinIO.
- `pip install -r requirements.txt` (duckdb, pandas, matplotlib ; pyspark fourni par le service).
- Optionnel : rôle Kubernetes « admin » sur le service pour lancer le Job (`k8s/`).

## Jeu de données
| | |
|---|---|
| Nom | Régularité mensuelle TGV par liaisons (AQST) — SNCF Voyageurs |
| URL | https://www.data.gouv.fr/datasets/regularite-mensuelle-tgv-par-liaisons (CSV : https://www.data.gouv.fr/api/1/datasets/r/91fe399d-cafa-4e72-8ba3-56d8717fdad4) |
| Licence | ODbL (Open Database License) |
| Date de téléchargement | AAAA-MM-JJ (voir `raw/.../_metadata.json` sur S3) |
| Taille | ~2,6 Mo, CSV `;`, 26 colonnes, ~XX XXX lignes (une ligne = une liaison × un mois) |

## Problématique
Quelles liaisons TGV sont les moins ponctuelles à l'arrivée depuis 2018 ?
La ponctualité s'est-elle dégradée d'une année sur l'autre ?
Quelle cause de retard domine sur les liaisons les plus touchées ?

## Chemins S3 (bucket personnel `user-<identifiant>`, stockage SeaweedFS)
- Brut : `s3://<BUCKET>/bigdata_project/raw/sncf_regularite_tgv/` (CSV + `_metadata.json` avec sha256)
- Traité : `s3://<BUCKET>/bigdata_project/processed/`
  - `silver/regularite_tgv_clean.parquet` — données nettoyées
  - `gold/gold_liaisons.{parquet,csv}` — **dataset final** (classement des liaisons)
  - `gold/gold_liaison_annee.*`, `gold/gold_annuel.*` — évolution annuelle
  - `quality/rapport_qualite.csv` — problèmes de qualité et nombre de lignes touchées
  - `benchmark/benchmark.csv` — mesures DuckDB / pandas / Spark ; `_runs/` — journaux d'exécution

## Reproduction
```bash
git clone <URL_DU_DEPOT> && cd bigdata_project
pip install -r requirements.txt
export BUCKET=user-<identifiant_onyxia>   # ex. user-l-khlif-ece
bash src/run_all.sh            # tout : ingestion -> pipeline -> vérification -> benchmark -> figures
```
Étapes séparées (chacune relançable, les sorties sont écrasées, donc idempotentes) :
```bash
source src/config.sh
bash src/01_ingest.sh                                   # télécharge, contrôle taille/colonnes, dépose sur S3
python src/02_pipeline_duckdb.py --bucket $BUCKET       # brut S3 -> silver/gold S3
python src/03_verify_pandas.py data/raw/regularite_mensuelle_tgv_liaisons.csv data/gold/gold_liaisons.csv
python src/04_benchmark.py data/raw/regularite_mensuelle_tgv_liaisons.csv
python src/05_figures.py data/gold
```
Reprise après erreur : relancer `run_all.sh` (le brut déjà présent sur S3 est réutilisé ; `FORCE_DOWNLOAD=1` pour re-télécharger).
Variante Kubernetes : voir l'en-tête de `k8s/job-pipeline.yaml` puis `kubectl apply -f k8s/job-pipeline.yaml`.
Les preuves (captures numérotées) sont dans `evidence/`.
