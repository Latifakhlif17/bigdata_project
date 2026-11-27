"""Étape 3 — Vérification INDÉPENDANTE d'un résultat.

On recalcule le classement des liaisons avec un autre outil (pandas, code écrit
séparément, sans SQL ni DuckDB), directement depuis le CSV brut, puis on compare
avec la table gold produite par le pipeline. On affiche aussi le détail mensuel de
la pire liaison pour pouvoir refaire le calcul à la main (tableur / calculatrice).

Usage :
    aws s3 cp s3://$BUCKET/bigdata_project/raw/sncf_regularite_tgv/regularite_mensuelle_tgv_liaisons.csv data/raw/
    aws s3 cp s3://$BUCKET/bigdata_project/processed/gold/gold_liaisons.csv data/gold/
    python src/03_verify_pandas.py data/raw/regularite_mensuelle_tgv_liaisons.csv data/gold/gold_liaisons.csv
"""
import sys
import unicodedata

import pandas as pd

from schema import COLUMNS, MIN_CIRCULATIONS


def normalise(s):
    if pd.isna(s):
        return ""
    s = " ".join(str(s).split()).upper()
    return "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")


def to_num(series):
    return pd.to_numeric(series.astype(str).str.strip().str.replace(",", ".", regex=False),
                         errors="coerce")


def recompute(raw_path):
    df = pd.read_csv(raw_path, sep=";", dtype=str, keep_default_na=False, encoding="utf-8-sig")
    df.columns = COLUMNS
    df["mois"] = pd.to_datetime(df["date_raw"].str.strip().str[:7], format="%Y-%m", errors="coerce")
    df["dep"] = df["gare_depart_raw"].map(normalise)
    df["arr"] = df["gare_arrivee_raw"].map(normalise)
    for c in ["nb_prevus", "nb_annules", "nb_retard_arrivee"]:
        df[c] = to_num(df[c])
    df["nb_annules"] = df["nb_annules"].fillna(0)
    df["circules"] = df["nb_prevus"] - df["nb_annules"]

    ok = (
        df["mois"].notna()
        & ~df["dep"].isin(["", "0"]) & ~df["arr"].isin(["", "0"]) & (df["dep"] != df["arr"])
        & (df["nb_prevus"] > 0)
        & (df["nb_annules"] <= df["nb_prevus"])
        & ~(df["nb_retard_arrivee"] > df["circules"])
        & df["nb_retard_arrivee"].notna()
    )
    dup = df.duplicated(subset=["mois", "dep", "arr"], keep="first")
    clean = df[ok & ~dup].copy()
    print(f"[pandas] lignes brutes = {len(df)} | conservées = {len(clean)}")

    clean["liaison"] = clean["dep"] + " -> " + clean["arr"]
    g = clean.groupby("liaison").agg(nb_circules=("circules", "sum"),
                                     nb_retard_arrivee=("nb_retard_arrivee", "sum"))
    g = g[g["nb_circules"] >= MIN_CIRCULATIONS]
    g["regularite"] = 1 - g["nb_retard_arrivee"] / g["nb_circules"]
    return clean, g.sort_values("regularite")


def main(raw_path, gold_path):
    clean, mine = recompute(raw_path)
    gold = pd.read_csv(gold_path).set_index("liaison")

    both = mine.join(gold[["nb_circules", "nb_retard_arrivee", "regularite"]],
                     rsuffix="_pipeline", how="outer")
    both["ecart"] = (both["regularite"] - both["regularite_pipeline"]).abs()
    missing = both["ecart"].isna().sum()
    max_gap = both["ecart"].max()

    print(f"\nLiaisons : pandas = {len(mine)} | pipeline = {len(gold)} | non appariées = {missing}")
    print(f"Écart maximal de régularité : {max_gap:.2e}")
    top_mine = list(mine.head(10).index)
    top_gold = list(gold.sort_values("rang").head(10).index)
    print(f"Top 10 identique (mêmes liaisons) : {set(top_mine) == set(top_gold)}")

    worst = top_mine[0]
    detail = clean[clean["liaison"] == worst][["mois", "nb_prevus", "nb_annules",
                                               "circules", "nb_retard_arrivee"]]
    print(f"\n=== Détail de la pire liaison : {worst} (calcul à la main possible) ===")
    print(detail.sort_values("mois").to_string(index=False))
    s_c, s_r = detail["circules"].sum(), detail["nb_retard_arrivee"].sum()
    print(f"Σ circulés = {s_c:.0f} ; Σ retards = {s_r:.0f} ; "
          f"régularité = 1 - {s_r:.0f}/{s_c:.0f} = {1 - s_r / s_c:.6f}")
    print(f"Valeur du pipeline : {gold.loc[worst, 'regularite']:.6f}")

    passed = missing == 0 and max_gap < 1e-5 and set(top_mine) == set(top_gold)
    print("\nRÉSULTAT DE LA VÉRIFICATION :", "OK ✅" if passed else "ÉCHEC ❌")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2]))
