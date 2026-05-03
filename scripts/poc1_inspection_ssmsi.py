"""
POC SSMSI — Étape 1 : Inspection du fichier Parquet base communale.
"""
import urllib.request
from pathlib import Path
import pandas as pd

URL_PARQUET = "https://www.data.gouv.fr/api/1/datasets/r/604d71b8-337d-4869-9226-49e01bae87df"
DEST = Path("/tmp/ssmsi_communal.parquet")

if not DEST.exists():
    print(f"Téléchargement vers {DEST}...")
    urllib.request.urlretrieve(URL_PARQUET, DEST)
    print(f"OK — taille : {DEST.stat().st_size / 1e6:.1f} Mo")
else:
    print(f"Fichier déjà présent : {DEST}")

print("\nLecture du Parquet...")
df = pd.read_parquet(DEST)

print("\n========== STRUCTURE ==========")
print(f"Shape : {df.shape}")
print(f"\nColonnes ({len(df.columns)}):")
for col in df.columns:
    print(f"  - {col}")

print("\n========== TYPES ==========")
print(df.dtypes)

print("\n========== HEAD (5 premières lignes) ==========")
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)
print(df.head())

print("\n========== STATS UTILES ==========")
for col in df.columns:
    if "indicateur" in col.lower() or "classe" in col.lower():
        print(f"\nValeurs uniques de '{col}' (max 20):")
        print(df[col].unique()[:20])
    if "annee" in col.lower() or "an" == col.lower():
        print(f"\nPlage de '{col}': {df[col].min()} → {df[col].max()}")
    if "code" in col.lower() and "commune" in col.lower():
        print(f"\nÉchantillon de '{col}' (5 valeurs): {df[col].head().tolist()}")

print("\n========== VALEURS MANQUANTES ==========")
print(df.isna().sum())

print("\n========== FIN INSPECTION ==========")