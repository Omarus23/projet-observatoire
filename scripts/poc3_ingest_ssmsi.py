"""
POC SSMSI — Étape 3 : Ingestion complète dans la base SQLite.

Pré-requis :
  - Base initialisée : python scripts/poc2_init_schema.py
  - pandas et pyarrow installés

Usage : python scripts/poc3_ingest_ssmsi.py

Idempotent : peut être relancé. Réingère le millésime si le fichier
a changé, skip sinon.
"""
import sys
from pathlib import Path

# Ajouter la racine du projet au PYTHONPATH pour pouvoir importer 'etl'
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from etl.db import get_connection
from etl.sources.ssmsi import ingest_ssmsi


def main() -> None:
    conn = get_connection()
    try:
        ingest_ssmsi(conn)

        # Vérifications post-ingestion
        print("\n" + "=" * 60)
        print("VÉRIFICATIONS POST-INGESTION")
        print("=" * 60)

        for table in ["sources", "communes", "indicateurs", "millesimes", "faits_indicateurs"]:
            nb = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table:<25} : {nb:>10,} lignes")

        # Échantillon de vérification : Villeurbanne 2024 cambriolages
        print("\n📊 Exemple : Villeurbanne (69266) en 2024")
        rows = conn.execute(
            """
            SELECT i.libelle, f.taux, f.valeur_brute, f.secret_stat
            FROM faits_indicateurs f
            JOIN indicateurs i ON i.id = f.indicateur_id
            WHERE f.code_insee = '69266' AND f.annee = 2024
            ORDER BY f.taux DESC NULLS LAST
            LIMIT 5
            """
        ).fetchall()
        for r in rows:
            secret = "🔒 secret" if r["secret_stat"] else ""
            taux = f"{r['taux']:.2f}/1000" if r["taux"] is not None else "N/A"
            nb = f"{int(r['valeur_brute']):,}" if r["valeur_brute"] else "N/A"
            print(f"    {r['libelle']:<45} {taux:>15} (n={nb}) {secret}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()