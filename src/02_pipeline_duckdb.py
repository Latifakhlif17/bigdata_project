"""Étape 2 — Pipeline principal (DuckDB) : brut S3 -> bronze -> silver -> gold -> S3.

Question : quelles liaisons TGV sont les moins ponctuelles à l'arrivée,
la situation s'est-elle dégradée, et quelle cause de retard domine ?

Usage (sur Onyxia, les identifiants S3 sont déjà dans l'environnement) :
    python src/02_pipeline_duckdb.py --bucket $BUCKET
Mode local (test sans S3) :
    python src/02_pipeline_duckdb.py --raw data/raw/x.csv --out data/processed
"""
import argparse
import json
import os
import socket
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from schema import CAUSES, COLUMNS, MIN_CIRCULATIONS

NUMERIC = [c for c in COLUMNS if c not in
           ("date_raw", "service", "gare_depart_raw", "gare_arrivee_raw",
            "comm_annulations", "comm_retard_depart", "comm_retard_arrivee")]
INTS = {"nb_prevus", "nb_annules", "nb_retard_depart", "nb_retard_arrivee",
        "nb_retard_15", "nb_retard_30", "nb_retard_60"}


def connect_s3(con):
    """Configure l'accès au MinIO d'Onyxia à partir des variables d'environnement."""
    con.execute("INSTALL httpfs; LOAD httpfs;")
    endpoint = os.environ.get("AWS_S3_ENDPOINT", "minio.lab.sspcloud.fr")
    endpoint = endpoint.replace("https://", "").replace("http://", "")
    con.execute(f"""
        CREATE OR REPLACE SECRET s3_onyxia (
            TYPE S3,
            KEY_ID '{os.environ["AWS_ACCESS_KEY_ID"]}',
            SECRET '{os.environ["AWS_SECRET_ACCESS_KEY"]}',
            SESSION_TOKEN '{os.environ.get("AWS_SESSION_TOKEN", "")}',
            ENDPOINT '{endpoint}',
            REGION '{os.environ.get("AWS_DEFAULT_REGION", "us-east-1")}',
            URL_STYLE 'path'
        );""")


def norm_gare(col):
    # trim + espaces multiples -> 1 + majuscules + suppression des accents
    return f"upper(strip_accents(trim(regexp_replace({col}, '\\s+', ' ', 'g'))))"


def run(con, raw, out):
    t = {}
    names = ", ".join(f"'{c}'" for c in COLUMNS)

    # ---------- BRONZE : lecture brute, tout en texte, rien n'est perdu ----------
    t0 = time.perf_counter()
    con.execute(f"""
        CREATE OR REPLACE TABLE bronze AS
        SELECT row_number() OVER () AS row_id, *
        FROM read_csv('{raw}', delim=';', header=true, quote='"',
                      all_varchar=true, names=[{names}]);""")
    t["bronze_s"] = time.perf_counter() - t0
    n_raw = con.execute("SELECT count(*) FROM bronze").fetchone()[0]
    print(f"[bronze] {n_raw} lignes lues")

    # ---------- TYPAGE + DRAPEAUX QUALITÉ ----------
    t0 = time.perf_counter()
    casts = ",\n".join(
        (f"TRY_CAST(TRY_CAST(replace(trim({c}), ',', '.') AS DOUBLE) AS BIGINT) AS {c}" if c in INTS
         else f"TRY_CAST(replace(trim({c}), ',', '.') AS DOUBLE) AS {c}")
        for c in NUMERIC)
    bad_cast = " OR ".join(
        f"(nullif(trim({c}), '') IS NOT NULL AND TRY_CAST(replace(trim({c}), ',', '.') AS DOUBLE) IS NULL)"
        for c in NUMERIC)
    con.execute(f"""
        CREATE OR REPLACE TABLE typed AS
        SELECT row_id,
               CAST(coalesce(try_strptime(trim(date_raw), '%Y-%m'),
                             try_strptime(trim(date_raw), '%Y-%m-%d')) AS DATE) AS mois,
               trim(service) AS service,
               gare_depart_raw, gare_arrivee_raw,
               {norm_gare('gare_depart_raw')}  AS gare_depart,
               {norm_gare('gare_arrivee_raw')} AS gare_arrivee,
               {casts},
               ({bad_cast}) AS q_cast_invalide
        FROM bronze;""")

    sum_causes = " + ".join(f"coalesce({c}, 0)" for c in CAUSES)
    con.execute(f"""
        CREATE OR REPLACE TABLE flagged AS
        SELECT *,
          mois IS NULL                                                AS q_date_invalide,
          (gare_depart IN ('', '0') OR gare_arrivee IN ('', '0')
             OR gare_depart IS NULL OR gare_arrivee IS NULL
             OR gare_depart = gare_arrivee)                           AS q_gare_invalide,
          (nb_prevus IS NULL OR nb_prevus <= 0)                       AS q_circulations_nulles,
          coalesce(nb_annules > nb_prevus, false)                     AS q_annules_incoherent,
          coalesce(nb_retard_arrivee > nb_prevus - coalesce(nb_annules,0), false)
                                                                      AS q_retards_incoherent,
          coalesce(nb_retard_15 < nb_retard_30 OR nb_retard_30 < nb_retard_60, false)
                                                                      AS q_seuils_incoherent,
          (({sum_causes}) > 0 AND abs(({sum_causes}) - 100) > 1)      AS q_causes_somme_100,
          (gare_depart_raw IS DISTINCT FROM gare_depart
             OR gare_arrivee_raw IS DISTINCT FROM gare_arrivee)       AS q_nom_gare_normalise,
          row_number() OVER (PARTITION BY mois, gare_depart, gare_arrivee
                             ORDER BY row_id) > 1                     AS q_doublon
        FROM typed;""")
    t["typage_qualite_s"] = time.perf_counter() - t0

    # ---------- RAPPORT QUALITÉ ----------
    issues = [
        ("q_date_invalide", "Date non interprétable", "ligne exclue"),
        ("q_gare_invalide", "Gare vide, '0' ou départ = arrivée", "ligne exclue"),
        ("q_cast_invalide", "Valeur numérique illisible", "valeur mise à NULL"),
        ("q_circulations_nulles", "Circulations prévues nulles ou absentes", "ligne exclue"),
        ("q_annules_incoherent", "Annulés > prévus", "ligne exclue"),
        ("q_retards_incoherent", "Retards arrivée > trains ayant circulé", "ligne exclue"),
        ("q_doublon", "Doublon (mois, départ, arrivée)", "seule la 1re occurrence est gardée"),
        ("q_seuils_incoherent", "Retards >15 < >30 ou >30 < >60", "ligne gardée, signalée"),
        ("q_causes_somme_100", "Somme des % de causes ≠ 100 (±1)", "causes mises à NULL"),
        ("q_nom_gare_normalise", "Nom de gare modifié (espaces/accents/casse)", "nom normalisé"),
    ]
    union = " UNION ALL ".join(
        f"SELECT '{c}' AS controle, '{d}' AS description, "
        f"count(*) FILTER (WHERE {c}) AS nb_lignes, '{a}' AS traitement FROM flagged"
        for c, d, a in issues)
    con.execute(f"CREATE OR REPLACE TABLE rapport_qualite AS {union};")
    print(con.execute("SELECT controle, nb_lignes FROM rapport_qualite").df().to_string(index=False))

    # ---------- SILVER : données propres ----------
    t0 = time.perf_counter()
    causes_sel = ",\n".join(f"CASE WHEN q_causes_somme_100 THEN NULL ELSE {c} END AS {c}" for c in CAUSES)
    con.execute(f"""
        CREATE OR REPLACE TABLE silver AS
        SELECT mois, year(mois) AS annee, service, gare_depart, gare_arrivee,
               gare_depart || ' -> ' || gare_arrivee AS liaison,
               duree_moyenne, nb_prevus, coalesce(nb_annules, 0) AS nb_annules,
               nb_prevus - coalesce(nb_annules, 0) AS nb_circules,
               nb_retard_arrivee, retard_moy_tous_arrivee,
               nb_retard_15, nb_retard_30, nb_retard_60,
               {causes_sel},
               1 - nb_retard_arrivee / nullif(nb_prevus - coalesce(nb_annules, 0), 0) AS regularite,
               q_seuils_incoherent
        FROM flagged
        WHERE NOT (q_date_invalide OR q_gare_invalide OR q_circulations_nulles
                   OR q_annules_incoherent OR q_retards_incoherent OR q_doublon)
          AND nb_retard_arrivee IS NOT NULL;""")
    t["silver_s"] = time.perf_counter() - t0
    n_silver = con.execute("SELECT count(*) FROM silver").fetchone()[0]
    print(f"[silver] {n_silver} lignes conservées ({n_raw - n_silver} exclues)")

    # ---------- GOLD ----------
    t0 = time.perf_counter()
    cause_w = ",\n".join(
        f"sum({c} * nb_retard_arrivee) / nullif(sum(CASE WHEN {c} IS NOT NULL "
        f"THEN nb_retard_arrivee END), 0) AS {c}" for c in CAUSES)
    greatest = ", ".join(f"coalesce({c}, -1)" for c in CAUSES)
    dominant = "\n".join(
        f"WHEN coalesce({c}, -1) = greatest({greatest}) THEN '{c.removeprefix('pct_')}'"
        for c in CAUSES)
    # Gold 1 : LE dataset final qui répond à la question (classement des liaisons)
    con.execute(f"""
        CREATE OR REPLACE TABLE gold_liaisons AS
        WITH agg AS (
          SELECT liaison, any_value(gare_depart) AS gare_depart,
                 any_value(gare_arrivee) AS gare_arrivee, mode(service) AS service,
                 count(*) AS nb_mois, min(mois) AS premier_mois, max(mois) AS dernier_mois,
                 sum(nb_prevus) AS nb_prevus, sum(nb_annules) AS nb_annules,
                 sum(nb_circules) AS nb_circules, sum(nb_retard_arrivee) AS nb_retard_arrivee,
                 sum(nb_retard_60) AS nb_retard_60,
                 sum(retard_moy_tous_arrivee * nb_circules) / sum(nb_circules) AS retard_moyen_min,
                 {cause_w}
          FROM silver GROUP BY liaison
          HAVING sum(nb_circules) >= {MIN_CIRCULATIONS})
        SELECT row_number() OVER (ORDER BY 1 - nb_retard_arrivee / nb_circules) AS rang,
               *,
               round(1 - nb_retard_arrivee / nb_circules, 6) AS regularite,
               round(nb_annules / nb_prevus, 6) AS taux_annulation,
               CASE WHEN greatest({greatest}) < 0 THEN NULL {dominant} END AS cause_dominante
        FROM agg ORDER BY rang;""")
    # Gold 2 : évolution annuelle par liaison
    con.execute("""
        CREATE OR REPLACE TABLE gold_liaison_annee AS
        SELECT liaison, annee, count(*) AS nb_mois, sum(nb_circules) AS nb_circules,
               sum(nb_retard_arrivee) AS nb_retard_arrivee,
               round(1 - sum(nb_retard_arrivee) / sum(nb_circules), 6) AS regularite
        FROM silver GROUP BY liaison, annee ORDER BY liaison, annee;""")
    # Gold 3 : tendance annuelle (médiane des liaisons : la SNCF déconseille de sommer
    # les liaisons pour une régularité globale car un train compte dans plusieurs liaisons)
    con.execute("""
        CREATE OR REPLACE TABLE gold_annuel AS
        SELECT annee, count(*) AS nb_liaisons, min(nb_mois) AS nb_mois_min,
               round(median(regularite), 6) AS regularite_mediane_liaisons,
               round(quantile_cont(regularite, 0.1), 6) AS regularite_p10
        FROM gold_liaison_annee WHERE nb_circules >= 100
        GROUP BY annee ORDER BY annee;""")
    t["gold_s"] = time.perf_counter() - t0

    print("\n=== Top 10 des liaisons les MOINS ponctuelles ===")
    print(con.execute("""SELECT rang, liaison, nb_circules, regularite, retard_moyen_min,
                         cause_dominante FROM gold_liaisons WHERE rang <= 10""").df().to_string(index=False))
    print("\n=== Tendance annuelle ===")
    print(con.execute("SELECT * FROM gold_annuel").df().to_string(index=False))

    # ---------- ÉCRITURE ----------
    t0 = time.perf_counter()
    if not out.startswith("s3://"):
        for sub in ("silver", "gold", "quality", "_runs"):
            Path(out, sub).mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY silver TO '{out}/silver/regularite_tgv_clean.parquet' (FORMAT parquet);")
    for tbl in ("gold_liaisons", "gold_liaison_annee", "gold_annuel"):
        con.execute(f"COPY {tbl} TO '{out}/gold/{tbl}.parquet' (FORMAT parquet);")
        con.execute(f"COPY {tbl} TO '{out}/gold/{tbl}.csv' (HEADER, DELIMITER ',');")
    con.execute(f"COPY rapport_qualite TO '{out}/quality/rapport_qualite.csv' (HEADER, DELIMITER ',');")
    t["ecriture_s"] = time.perf_counter() - t0
    return n_raw, n_silver, t


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bucket", help="bucket Onyxia (mode S3)")
    p.add_argument("--raw", help="chemin du CSV brut (mode local)")
    p.add_argument("--out", help="dossier de sortie (mode local)")
    a = p.parse_args()

    con = duckdb.connect()
    if a.bucket:
        connect_s3(con)
        raw = f"s3://{a.bucket}/bigdata_project/raw/sncf_regularite_tgv/regularite_mensuelle_tgv_liaisons.csv"
        out = f"s3://{a.bucket}/bigdata_project/processed"
    else:
        raw, out = a.raw, a.out

    ns_file = Path("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
    started = datetime.now(timezone.utc)
    t0 = time.perf_counter()
    n_raw, n_silver, timings = run(con, raw, out)
    log = {
        "debut_utc": started.isoformat(), "duree_totale_s": round(time.perf_counter() - t0, 3),
        "pod": socket.gethostname(),
        "namespace": ns_file.read_text().strip() if ns_file.exists() else None,
        "moteur": f"duckdb {duckdb.__version__}", "entree": raw, "sortie": out,
        "lignes_brutes": n_raw, "lignes_silver": n_silver,
        "durees_etapes_s": {k: round(v, 3) for k, v in timings.items()},
    }
    # Journal d'exécution écrit à côté des résultats (preuve d'exécution horodatée)
    flat = {k: (json.dumps(v) if isinstance(v, dict) else v) for k, v in log.items()}
    cols = ", ".join(f"?::VARCHAR AS {k}" for k in flat)
    con.execute(f"CREATE OR REPLACE TABLE run_log AS SELECT {cols}", list(map(str, flat.values())))
    stamp = started.strftime("%Y%m%dT%H%M%SZ")
    con.execute(f"COPY run_log TO '{out}/_runs/run_{stamp}.json' (FORMAT json);")
    print("\n" + json.dumps(log, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
