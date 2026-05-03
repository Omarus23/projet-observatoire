"""
POC SSMSI — Étape 2 : Initialisation du schéma SQLite.

Ce script :
  1. Crée le dossier data/processed/ si nécessaire
  2. Crée (ou ouvre) la base observatoire.db
  3. Exécute le script DDL etl/schema.sql
  4. Vérifie que toutes les tables ont été créées

Idempotent : peut être relancé plusieurs fois sans dommage.
"""
import sqlite3
import sys
from pathlib import Path

# --- Chemins ---
# BASE_DIR = racine du projet (parent de scripts/)
BASE_DIR = Path(__file__).resolve().parent.parent
SCHEMA_FILE = BASE_DIR / "etl" / "schema.sql"
DB_DIR = BASE_DIR / "data" / "processed"
DB_PATH = DB_DIR / "observatoire.db"


def main() -> None:
    # 1. Vérifier que le fichier schema.sql existe
    if not SCHEMA_FILE.exists():
        print(f"❌ Fichier introuvable : {SCHEMA_FILE}", file=sys.stderr)
        sys.exit(1)

    # 2. Créer le dossier data/processed/ si absent
    DB_DIR.mkdir(parents=True, exist_ok=True)
    print(f"✅ Dossier prêt : {DB_DIR}")

    # 3. Lire le DDL
    schema_sql = SCHEMA_FILE.read_text(encoding="utf-8")
    print(f"✅ Schéma lu : {len(schema_sql)} caractères")

    # 4. Connexion SQLite + activation des contraintes FK
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")

    # 5. Exécution du DDL
    try:
        conn.executescript(schema_sql)
        conn.commit()
        print(f"✅ Schéma appliqué à {DB_PATH}")
    except sqlite3.Error as e:
        print(f"❌ Erreur SQL : {e}", file=sys.stderr)
        conn.close()
        sys.exit(1)

    # 6. Vérification : lister les tables créées
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    tables = [row[0] for row in cursor.fetchall()]
    print(f"\n📋 Tables présentes dans la DB ({len(tables)}):")
    for t in tables:
        cursor = conn.execute(f"SELECT COUNT(*) FROM {t}")
        nb = cursor.fetchone()[0]
        print(f"   - {t} ({nb} lignes)")

    # 7. Vérification spéciale : la source SSMSI doit être présente
    cursor = conn.execute("SELECT code, libelle FROM sources WHERE code = 'ssmsi'")
    row = cursor.fetchone()
    if row:
        print(f"\n✅ Source SSMSI référencée : {row[1]}")
    else:
        print("⚠️  Source SSMSI absente — anomalie", file=sys.stderr)

    conn.close()
    print(f"\n🎉 Schéma initialisé avec succès : {DB_PATH}")


if __name__ == "__main__":
    main()