# Captures d'écran à produire

Avant CHAQUE capture, lancer `bash src/whoami.sh` dans le terminal pour que
l'identifiant Onyxia, le namespace et la date/heure soient visibles à l'écran.

| Fichier | Ce qu'on doit voir |
|---|---|
| `01_onyxia_services.png` | La page « Mes services » d'Onyxia (identifiant visible en haut) |
| `02_ingest_trace.png` | Sortie de `bash src/01_ingest.sh` (taille, colonnes, `mc ls`) |
| `03_s3_raw.png` | `mc ls --recursive s3/$BUCKET/bigdata_project/raw/` ou l'explorateur de fichiers Onyxia |
| `04_pipeline_trace.png` | Sortie de `02_pipeline_duckdb.py` (rapport qualité + top 10 + JSON du run) |
| `05_s3_processed.png` | `mc ls --recursive s3/$BUCKET/bigdata_project/processed/` |
| `06_verification.png` | Sortie de `03_verify_pandas.py` avec « RÉSULTAT DE LA VÉRIFICATION : OK » |
| `07_verification_manuelle.png` | Le calcul à la main de la pire liaison (tableur) |
| `08_benchmark_trace.png` | Sortie de `04_benchmark.py` (DuckDB, pandas, Spark, x1 et x100) |
| `09_spark_ui.png` | (option) Interface Spark UI pendant le benchmark |
| `10_k8s_job.png` | (option) `kubectl get jobs,pods` + `kubectl logs job/tgv-pipeline` |
