"""Schéma du fichier brut SNCF « Régularité mensuelle TGV par liaisons ».

Les en-têtes d'origine sont longs, accentués et ont changé au fil des versions :
on renomme donc les 26 colonnes par POSITION avec des noms courts.
"""

COLUMNS = [
    "date_raw",                      # 1  Date (AAAA-MM)
    "service",                       # 2  National / International
    "gare_depart_raw",               # 3
    "gare_arrivee_raw",              # 4
    "duree_moyenne",                 # 5  Durée moyenne du trajet (min)
    "nb_prevus",                     # 6  Nombre de circulations prévues
    "nb_annules",                    # 7  Nombre de trains annulés
    "comm_annulations",              # 8
    "nb_retard_depart",              # 9
    "retard_moy_retardes_depart",    # 10 (min)
    "retard_moy_tous_depart",        # 11 (min)
    "comm_retard_depart",            # 12
    "nb_retard_arrivee",             # 13 Nombre de trains en retard à l'arrivée
    "retard_moy_retardes_arrivee",   # 14 (min)
    "retard_moy_tous_arrivee",       # 15 (min)
    "comm_retard_arrivee",           # 16
    "nb_retard_15",                  # 17 trains en retard > 15 min
    "retard_moy_15",                 # 18
    "nb_retard_30",                  # 19 > 30 min
    "nb_retard_60",                  # 20 > 60 min
    "pct_externes",                  # 21 % retards causes externes (météo, obstacles...)
    "pct_infrastructure",            # 22 % infrastructure (voies, signalisation)
    "pct_gestion_trafic",            # 23 % gestion du trafic
    "pct_materiel",                  # 24 % matériel roulant
    "pct_gare",                      # 25 % gestion en gare / réutilisation matériel
    "pct_voyageurs",                 # 26 % prise en compte voyageurs
]

CAUSES = [
    "pct_externes",
    "pct_infrastructure",
    "pct_gestion_trafic",
    "pct_materiel",
    "pct_gare",
    "pct_voyageurs",
]

# Une liaison doit cumuler au moins ce nombre de trains pour être classée
# (évite qu'une liaison avec 3 trains en tout arrive en tête du classement).
MIN_CIRCULATIONS = 1000
