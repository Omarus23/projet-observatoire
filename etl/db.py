"""
Helpers de connexion à la base SQLite.

Centralise la création de connexions avec les bons pragmas
(foreign_keys, performance) pour qu'on n'ait pas à les répéter
partout.
"""
import sqlite3
from pathlib import Path

# Chemin de la DB (résolu depuis la racine du projet)
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "processed" / "observatoire.db"


def get_connection() -> sqlite3.Connection:
    """
    Retourne une connexion SQLite avec les pragmas standard du projet.

    Pragmas appliqués :
      - foreign_keys = ON  : applique les contraintes FK (off par défaut SQLite)
      - journal_mode = WAL : meilleure perf en lecture concurrente
      - synchronous = NORMAL : compromis perf/sécurité raisonnable

    Returns:
        sqlite3.Connection prête à l'usage
    """
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Base introuvable : {DB_PATH}. "
            "Lance d'abord : python scripts/poc2_init_schema.py"
        )

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")

    # Faciliter l'accès par nom de colonne (row['code_insee'] au lieu de row[0])
    conn.row_factory = sqlite3.Row

    return conn