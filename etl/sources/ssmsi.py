"""
Module d'ingestion SSMSI — Bases statistiques communales de la délinquance.

Source : SSMSI (Service Statistique Ministériel de la Sécurité Intérieure)
URL    : https://www.data.gouv.fr/datasets/bases-statistiques-communale-departementale-et-regionale-de-la-delinquance-enregistree-par-la-police-et-la-gendarmerie-nationales
Format : Parquet, ~16 Mo, 5.2M lignes, 13 colonnes, 2016-2025

Stratégie :
  1. Télécharger le fichier (skip si déjà présent et hash inchangé)
  2. Lire avec pandas
  3. Peupler les 18 indicateurs SSMSI (mapping manuel)
  4. Peupler les communes (codes INSEE harmonisés CODGEO_2025)
  5. Pour chaque année : insérer millésime + faits par batch de 10 000
"""
import hashlib
import sqlite3
import time
import urllib.request
from datetime import datetime
from pathlib import Path

import pandas as pd

# ============================================================
# CONSTANTES
# ============================================================

URL_PARQUET = "https://www.data.gouv.fr/api/1/datasets/r/604d71b8-337d-4869-9226-49e01bae87df"

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DEST_PARQUET = BASE_DIR / "data" / "raw" / "ssmsi" / "base_communale.parquet"

# Mapping libellé SSMSI → (code court, dimension)
# Codes choisis pour être courts, stables et lisibles dans les URLs API.
INDICATEURS_SSMSI = {
    # Violences aux personnes
    "Homicides":                                ("ssmsi_homicides",          "securite"),
    "Tentatives d'homicide":                    ("ssmsi_tentatives_homicide","securite"),
    "Violences physiques intrafamiliales":      ("ssmsi_violences_intrafam", "securite"),
    "Violences physiques hors cadre familial":  ("ssmsi_violences_horsfam",  "securite"),
    "Violences sexuelles":                      ("ssmsi_violences_sexuelles","securite"),

    # Vols
    "Vols avec armes":                          ("ssmsi_vols_armes",         "securite"),
    "Vols violents sans arme":                  ("ssmsi_vols_violents",      "securite"),
    "Vols sans violence contre des personnes":  ("ssmsi_vols_personnes",     "securite"),
    "Cambriolages de logement":                 ("ssmsi_cambriolages",       "securite"),
    "Vols de véhicules":                        ("ssmsi_vols_vehicules",     "securite"),
    "Vols dans les véhicules":                  ("ssmsi_vols_dans_vehicules","securite"),
    "Vols d'accessoires sur véhicules":         ("ssmsi_vols_accessoires",   "securite"),

    # Atteintes aux biens
    "Destructions et dégradations volontaires": ("ssmsi_destructions",       "securite"),

    # Stupéfiants
    "Usage de stupéfiants":                     ("ssmsi_usage_stups",        "securite"),
    "Usage de stupéfiants (AFD)":               ("ssmsi_usage_stups_afd",    "securite"),
    "Usage de stupéfiants (hors AFD)":          ("ssmsi_usage_stups_hors_afd","securite"),
    "Trafic de stupéfiants":                    ("ssmsi_trafic_stups",       "securite"),

    # Escroqueries
    "Escroqueries et fraudes aux moyens de paiement": ("ssmsi_escroqueries", "securite"),
}

BATCH_SIZE = 10_000


# ============================================================
# UTILITAIRES
# ============================================================

def calc_md5(path: Path) -> str:
    """Calcule le hash MD5 d'un fichier (pour détecter les changements)."""
    md5 = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            md5.update(chunk)
    return md5.hexdigest()


def telecharger_si_besoin(url: str, dest: Path) -> tuple[bool, str]:
    """
    Télécharge le fichier si absent. Retourne (a_telecharge, hash_md5).
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    if not dest.exists():
        print(f"  📥 Téléchargement → {dest}")
        urllib.request.urlretrieve(url, dest)
        print(f"  ✅ Taille : {dest.stat().st_size / 1e6:.1f} Mo")
        return True, calc_md5(dest)
    else:
        print(f"  📁 Fichier déjà présent : {dest}")
        return False, calc_md5(dest)


def get_source_id(conn: sqlite3.Connection, code: str) -> int:
    """Retourne l'id de la source par son code (échoue si absent)."""
    row = conn.execute("SELECT id FROM sources WHERE code = ?", (code,)).fetchone()
    if row is None:
        raise ValueError(f"Source '{code}' absente de la table sources")
    return row["id"]


def slug_canonique(nom: str) -> str:
    """Normalise un nom de commune pour la recherche (minuscules, sans accents)."""
    import unicodedata
    s = unicodedata.normalize("NFKD", nom).encode("ASCII", "ignore").decode()
    return s.lower().replace("-", " ").replace("'", " ").strip()


# ============================================================
# PEUPLEMENT DES TABLES DIMENSION
# ============================================================

def peupler_indicateurs(conn: sqlite3.Connection, source_id: int) -> dict[str, int]:
    """
    Insère les 18 indicateurs SSMSI dans la table indicateurs.
    Retourne un dict {libelle_ssmsi: indicateur_id}.
    """
    print("\n📋 Peuplement des indicateurs SSMSI...")
    rows = []
    for libelle, (code, dimension) in INDICATEURS_SSMSI.items():
        rows.append((code, libelle, dimension, source_id, "taux_pour_1000_hab"))

    conn.executemany(
        """
        INSERT OR IGNORE INTO indicateurs
            (code, libelle, dimension, source_id, unite)
        VALUES (?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()

    # Récupérer le mapping libelle → id
    mapping = {}
    for libelle in INDICATEURS_SSMSI.keys():
        code = INDICATEURS_SSMSI[libelle][0]
        row = conn.execute("SELECT id FROM indicateurs WHERE code = ?", (code,)).fetchone()
        mapping[libelle] = row["id"]

    print(f"  ✅ {len(mapping)} indicateurs présents en DB")
    return mapping


def peupler_communes(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    """
    Insère les communes uniques du fichier dans la table communes.
    Pour le POC, on remplit minimalement (code, nom, dept, region).
    Le nom détaillé viendra plus tard via le COG INSEE.
    """
    print("\n🏘️  Peuplement des communes...")
    codes_uniques = df["CODGEO_2025"].drop_duplicates().tolist()

    # Pour le POC, on n'a pas le nom des communes (pas dans le fichier SSMSI).
    # On stocke juste le code, on enrichira via le COG INSEE en V2.
    # Le département est extrait du code INSEE (2 ou 3 premiers caractères).
    rows = []
    for code in codes_uniques:
        if code.startswith("97"):  # DROM (3 chiffres de département)
            dept = code[:3]
        else:  # Métropole (2 chiffres) ou Corse (2A, 2B)
            dept = code[:2]
        # Région : à enrichir plus tard, on met "00" provisoire
        rows.append((code, f"Commune {code}", slug_canonique(f"commune {code}"), dept, "00"))

    conn.executemany(
        """
        INSERT OR IGNORE INTO communes
            (code_insee, nom, nom_canonique, code_dept, code_region)
        VALUES (?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    print(f"  ✅ {len(codes_uniques)} communes uniques traitées")
    return len(codes_uniques)


# ============================================================
# PEUPLEMENT DES FAITS
# ============================================================

def ingerer_annee(
    conn: sqlite3.Connection,
    df_annee: pd.DataFrame,
    annee: int,
    source_id: int,
    indicateurs_map: dict[str, int],
    fichier_path: str,
    hash_md5: str,
) -> int:
    """
    Ingère les faits d'une année donnée.
    Retourne le nombre de lignes insérées.
    """
    # 1. Insérer ou récupérer le millésime
    conn.execute(
        """
        INSERT OR REPLACE INTO millesimes
            (source_id, annee, date_ingestion, fichier_source, hash_md5, nb_lignes_ingerees)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (source_id, annee, datetime.now().isoformat(timespec="seconds"),
         fichier_path, hash_md5, 0),  # nb mis à jour à la fin
    )
    millesime_id = conn.execute(
        "SELECT id FROM millesimes WHERE source_id = ? AND annee = ?",
        (source_id, annee),
    ).fetchone()["id"]

    # 2. Supprimer les anciens faits de ce millésime (idempotence)
    conn.execute("DELETE FROM faits_indicateurs WHERE millesime_id = ?", (millesime_id,))

    # 3. Préparer les lignes à insérer
    rows = []
    for _, r in df_annee.iterrows():
        libelle = r["indicateur"]
        if libelle not in indicateurs_map:
            continue  # indicateur non mappé (ne devrait pas arriver)

        rows.append((
            r["CODGEO_2025"],
            indicateurs_map[libelle],
            int(r["annee"]),
            None if pd.isna(r["taux_pour_mille"]) else float(r["taux_pour_mille"]),
            None if pd.isna(r["nombre"]) else float(r["nombre"]),
            1 if r["est_diffuse"] == "ndiff" else 0,
            int(r["insee_pop"]) if not pd.isna(r["insee_pop"]) else None,
            int(r["insee_pop_millesime"]) if not pd.isna(r["insee_pop_millesime"]) else None,
            millesime_id,
        ))

    # 4. Insertion par batch
    nb_total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        conn.executemany(
            """
            INSERT INTO faits_indicateurs
                (code_insee, indicateur_id, annee, taux, valeur_brute,
                 secret_stat, population_ref, pop_millesime, millesime_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            batch,
        )
        nb_total += len(batch)

    # 5. Mise à jour du compteur de lignes du millésime
    conn.execute(
        "UPDATE millesimes SET nb_lignes_ingerees = ? WHERE id = ?",
        (nb_total, millesime_id),
    )

    return nb_total


# ============================================================
# POINT D'ENTRÉE PRINCIPAL
# ============================================================

def ingest_ssmsi(conn: sqlite3.Connection) -> None:
    """
    Pipeline complet d'ingestion SSMSI.
    Suppose que la DB existe avec son schéma initialisé.
    """
    debut = time.time()
    print("=" * 60)
    print("INGESTION SSMSI — Base communale de la délinquance")
    print("=" * 60)

    # 1. Télécharger le fichier
    a_telecharge, hash_md5 = telecharger_si_besoin(URL_PARQUET, DEST_PARQUET)
    print(f"  🔑 MD5 : {hash_md5[:16]}...")

    # 2. Lire le Parquet
    print("\n📖 Lecture du fichier Parquet...")
    df = pd.read_parquet(DEST_PARQUET)
    print(f"  ✅ {len(df):,} lignes, {df.shape[1]} colonnes")

    # 3. Récupérer l'id de la source
    source_id = get_source_id(conn, "ssmsi")

    # 4. Peupler les indicateurs et communes
    indicateurs_map = peupler_indicateurs(conn, source_id)
    peupler_communes(conn, df)

    # 5. Ingérer année par année
    annees = sorted(df["annee"].unique())
    print(f"\n📅 Ingestion par année ({len(annees)} millésimes)...")
    nb_total_global = 0
    for annee in annees:
        df_annee = df[df["annee"] == annee]
        t0 = time.time()
        nb = ingerer_annee(
            conn, df_annee, int(annee), source_id, indicateurs_map,
            str(DEST_PARQUET), hash_md5,
        )
        nb_total_global += nb
        duree = time.time() - t0
        print(f"  ✅ {annee} : {nb:>9,} lignes en {duree:.1f}s")

    # 6. Commit final
    conn.commit()
    duree_totale = time.time() - debut
    print(f"\n🎉 Ingestion terminée : {nb_total_global:,} lignes en {duree_totale:.1f}s")