-- ============================================================
-- Schéma de base de données — Projet Observatoire
-- ============================================================
-- Stockage des indicateurs territoriaux croisés (sécurité, revenus,
-- démographie, immobilier) à la maille communale française.
--
-- Version : 1.0 — POC SSMSI
-- Date    : 2026-05
-- Auteur  : Alpha-Omar ATTO
--
-- Conventions :
--   - Tous les libellés et codes en minuscules
--   - Snake_case pour les noms de tables et colonnes
--   - Codes INSEE stockés en TEXT (chaînes commençant par 0)
--   - Années en INTEGER
-- ============================================================

-- Activation des contraintes de clé étrangère (SQLite ne le fait
-- pas par défaut, c'est une discipline à activer explicitement)
PRAGMA foreign_keys = ON;

-- ============================================================
-- TABLES DE DIMENSION
-- ============================================================

-- Catalogue des sources de données utilisées
CREATE TABLE IF NOT EXISTS sources (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    code        TEXT UNIQUE NOT NULL,        -- 'ssmsi', 'filosofi', ...
    libelle     TEXT NOT NULL,               -- libellé long
    producteur  TEXT NOT NULL,               -- 'Ministère de l'Intérieur'
    url         TEXT NOT NULL,               -- URL de référence data.gouv
    licence     TEXT NOT NULL DEFAULT 'Licence Ouverte v2.0',
    frequence   TEXT                         -- 'annuelle', 'semestrielle'
);

-- Référentiel des communes (codes INSEE harmonisés au COG le plus récent)
CREATE TABLE IF NOT EXISTS communes (
    code_insee          TEXT PRIMARY KEY,    -- '69266' = Villeurbanne
    nom                 TEXT NOT NULL,
    nom_canonique       TEXT NOT NULL,       -- pour recherche : 'villeurbanne'
    code_dept           TEXT NOT NULL,       -- '69'
    code_region         TEXT NOT NULL,       -- '84' (ARA)
    type_unite_urbaine  TEXT,                -- 'commune-centre', 'banlieue', 'rurale'
    statut_plm          TEXT,                -- NULL ou 'arrondissement_paris', etc.
    latitude            REAL,
    longitude           REAL,
    actif               INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_communes_dept ON communes(code_dept);
CREATE INDEX IF NOT EXISTS idx_communes_canonique ON communes(nom_canonique);

-- Catalogue des indicateurs disponibles (un par source × type d'indicateur)
CREATE TABLE IF NOT EXISTS indicateurs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    code            TEXT UNIQUE NOT NULL,    -- 'ssmsi_violences_intrafam', ...
    libelle         TEXT NOT NULL,           -- libellé brut SSMSI/Insee
    dimension       TEXT NOT NULL,           -- 'securite', 'revenus', 'demographie', 'immobilier'
    source_id       INTEGER NOT NULL,
    unite           TEXT,                    -- 'taux_pour_1000_hab', 'taux_pour_1000_log', 'euros'
    unite_de_compte TEXT,                    -- AJOUT POC : 'Victime', 'Infraction', NULL
    description     TEXT,
    formule         TEXT,                    -- formule explicite
    actif           INTEGER NOT NULL DEFAULT 1,
    FOREIGN KEY (source_id) REFERENCES sources(id)
);
CREATE INDEX IF NOT EXISTS idx_indicateurs_dimension ON indicateurs(dimension);
CREATE INDEX IF NOT EXISTS idx_indicateurs_source ON indicateurs(source_id);

-- Référentiel des millésimes ingérés (traçabilité)
CREATE TABLE IF NOT EXISTS millesimes (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id          INTEGER NOT NULL,
    annee              INTEGER NOT NULL,
    date_publication   TEXT,                  -- ISO date "2026-03-26"
    date_ingestion     TEXT NOT NULL,         -- ISO datetime "2026-05-03T14:30:00"
    fichier_source     TEXT,                  -- chemin local du fichier ingéré
    hash_md5           TEXT,                  -- hash du fichier source
    nb_lignes_ingerees INTEGER,
    UNIQUE (source_id, annee),
    FOREIGN KEY (source_id) REFERENCES sources(id)
);
CREATE INDEX IF NOT EXISTS idx_millesimes_source ON millesimes(source_id);

-- ============================================================
-- TABLE DE FAITS
-- ============================================================

-- Stockage central des indicateurs par commune × année
CREATE TABLE IF NOT EXISTS faits_indicateurs (
    code_insee     TEXT NOT NULL,
    indicateur_id  INTEGER NOT NULL,
    annee          INTEGER NOT NULL,

    -- Valeurs principales
    taux           REAL,                       -- AJUSTEMENT POC : renommé 'valeur' → 'taux'
    valeur_brute   REAL,                       -- nb de faits absolus

    -- Métadonnées de diffusion
    secret_stat    INTEGER NOT NULL DEFAULT 0, -- 0 = diffusé, 1 = secret statistique
    population_ref INTEGER,                    -- pop ayant servi au calcul du taux
    pop_millesime  INTEGER,                    -- année de la pop de référence

    -- Traçabilité
    millesime_id   INTEGER NOT NULL,

    PRIMARY KEY (code_insee, indicateur_id, annee),
    FOREIGN KEY (code_insee)    REFERENCES communes(code_insee),
    FOREIGN KEY (indicateur_id) REFERENCES indicateurs(id),
    FOREIGN KEY (millesime_id)  REFERENCES millesimes(id)
);
CREATE INDEX IF NOT EXISTS idx_faits_commune_annee ON faits_indicateurs(code_insee, annee);
CREATE INDEX IF NOT EXISTS idx_faits_indicateur_annee ON faits_indicateurs(indicateur_id, annee);
CREATE INDEX IF NOT EXISTS idx_faits_millesime ON faits_indicateurs(millesime_id);

-- ============================================================
-- DONNÉES DE RÉFÉRENCE INITIALES
-- ============================================================

-- Insertion de la source SSMSI
INSERT OR IGNORE INTO sources (code, libelle, producteur, url, licence, frequence) VALUES
    ('ssmsi',
     'Bases statistiques communales de la délinquance enregistrée',
     'Service Statistique Ministériel de la Sécurité Intérieure',
     'https://www.data.gouv.fr/datasets/bases-statistiques-communale-departementale-et-regionale-de-la-delinquance-enregistree-par-la-police-et-la-gendarmerie-nationales',
     'Licence Ouverte v2.0',
     'annuelle');

-- Les 4 autres sources (Filosofi, RP, DEFM, DVF) seront insérées
-- au moment de leur intégration ETL respective.