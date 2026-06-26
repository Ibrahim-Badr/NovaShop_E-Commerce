"""
novashop_lib.py — STARTER ÉTUDIANT (à compléter)
Module B3 IA & Data — Outils ETL — Projet NovaShop (séances 3 & 4)

La "plomberie" est fournie (chemins, helpers, extract, chargement DB, audit).
À VOUS d'implémenter les fonctions marquées `# TODO` (elles lèvent NotImplementedError).
Une fois toutes les fonctions complétées, les deux DAGs s'exécutent de bout en bout.

Conseil : implémentez dans l'ordre des séances (voir README_etudiant.md).
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta

import pandas as pd

BASE_DIR = os.environ.get("ETL_BASE_DIR", "/opt/airflow")
DATA_DIR = os.path.join(BASE_DIR, "data")
OUT_DIR = os.path.join(BASE_DIR, "output")
STAGING = os.path.join(OUT_DIR, "staging")
WAREHOUSE = os.path.join(OUT_DIR, "warehouse.db")
REJECTS = os.path.join(STAGING, "rejects.csv")

SOURCES = {
    "customers": "customers_raw.csv",
    "customer_changes": "customer_changes_raw.csv",
    "products": "products_raw.csv",
    "orders": "orders_raw.csv",
    "order_items": "order_items_raw.csv",
    "returns": "returns_raw.csv",
}

os.makedirs(STAGING, exist_ok=True)


def _stg(name: str) -> str:
    return os.path.join(STAGING, f"{name}.csv")


# [FOURNI] Helper de quarantaine : routez vos lignes rejetées ici, avec un motif.
def _reject(source: str, df_bad: pd.DataFrame, key_col: str, reason: str):
    if df_bad is None or df_bad.empty:
        return
    rec = pd.DataFrame({
        "source": source,
        "business_key": df_bad[key_col].astype(str) if key_col in df_bad else "?",
        "reason": reason,
        "rejected_at": datetime.utcnow().isoformat(timespec="seconds"),
    })
    rec.to_csv(REJECTS, mode="a", header=not os.path.exists(REJECTS), index=False)
    print(f"[REJECT] {source}: {len(df_bad)} lignes -> '{reason}'")


# =========================================================================== #
# [FOURNI] EXTRACT
# =========================================================================== #
def extract_all(**_):
    if os.path.exists(REJECTS):
        os.remove(REJECTS)
    for name, fname in SOURCES.items():
        path = os.path.join(DATA_DIR, fname)
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        pd.read_csv(path, dtype=str).to_csv(_stg(f"{name}_raw"), index=False)
        print(f"[EXTRACT] {name} chargé en staging")


# =========================================================================== #
# SÉANCE 3 — NETTOYAGE (avec quarantaine via _reject)
# Pour chaque source : lire _stg("<x>_raw"), nettoyer, router les rejets,
# puis écrire la table propre dans _stg("stg_<x>").
# =========================================================================== #
def clean_customers(**_):
    # TODO : dédoublonner customer_id (rejeter les doublons), normaliser la casse
    #        (full_name/country/segment), email vide -> NA + flag email_manquant,
    #        signup_date jj/mm/aaaa -> ISO. Écrire stg_customers.
    raise NotImplementedError("clean_customers à implémenter")


def clean_customer_changes(**_):
    # TODO : parser change_date (rejeter les dates invalides), normaliser new_segment.
    #        Écrire stg_customer_changes.
    raise NotImplementedError("clean_customer_changes à implémenter")


def clean_products(**_):
    # TODO : typer unit_cost/list_price, category vide -> 'Non catégorisé',
    #        rejeter les produits sans list_price (non valorisables). Écrire stg_products.
    raise NotImplementedError("clean_products à implémenter")


def clean_orders(**_):
    # TODO : typer order_date (ISO), normaliser status/channel (minuscules),
    #        rejeter order_id nul. Écrire stg_orders.
    raise NotImplementedError("clean_orders à implémenter")


def clean_order_items(**_):
    # TODO : dédoublonner, typer quantity/discount_pct, rejeter quantité <= 0,
    #        rejeter product_id orphelin (absent de stg_products),
    #        enrichir unit_price depuis stg_products.list_price. Écrire stg_order_items.
    raise NotImplementedError("clean_order_items à implémenter")


def clean_returns(**_):
    # TODO : dédoublonner return_id, typer qty_returned (rejeter non numérique),
    #        rejeter order_item_id orphelin, typer return_date. Écrire stg_returns.
    raise NotImplementedError("clean_returns à implémenter")


# =========================================================================== #
# SÉANCE 3/4 — DIMENSION CLIENT EN SCD TYPE 2
# =========================================================================== #
def build_dim_customer_scd2(**_):
    # TODO : à partir de stg_customers (segment initial) + stg_customer_changes,
    #        construire une dimension historisée avec, par client, une ligne par
    #        version : customer_sk (clé technique), customer_id, segment,
    #        valid_from, valid_to, is_current.
    #        Version initiale : valid_from = '1900-01-01'. Dernière version :
    #        valid_to = '9999-12-31', is_current = 1. Écrire dim_customer_scd.
    raise NotImplementedError("build_dim_customer_scd2 à implémenter")


# =========================================================================== #
# SÉANCE 4 — DIMENSIONS SIMPLES + DIMENSION DATE
# =========================================================================== #
def build_dimensions(**_):
    # TODO : dim_product (depuis stg_products) ; dim_date sur la plage des
    #        order_date avec year, month, year_month, quarter. Écrire dim_product et dim_date.
    raise NotImplementedError("build_dimensions à implémenter")


# =========================================================================== #
# [FOURNI] SÉANCE 4 — chargement des tables de référence dans l'entrepôt
# =========================================================================== #
def load_reference_tables(**_):
    con = sqlite3.connect(WAREHOUSE)
    try:
        for t in ["dim_product", "dim_date", "dim_customer_scd"]:
            pd.read_csv(_stg(t)).to_sql(t, con, if_exists="replace", index=False)
        rej = pd.read_csv(REJECTS) if os.path.exists(REJECTS) else pd.DataFrame(
            columns=["source", "business_key", "reason", "rejected_at"])
        rej.to_sql("rejects", con, if_exists="replace", index=False)
        con.execute("""CREATE TABLE IF NOT EXISTS audit_runs (
            run_ds TEXT, table_name TEXT, rows_loaded INTEGER, status TEXT, logged_at TEXT)""")
        con.commit()
        print(f"[LOAD ref] dims + rejects ({len(rej)} en quarantaine)")
    finally:
        con.close()


# =========================================================================== #
# SÉANCE 4 — INCRÉMENTAL PAR JOUR
# =========================================================================== #
def has_new_data(ds: str) -> bool:
    # TODO : renvoyer True s'il existe au moins une commande 'completed' à la date ds.
    raise NotImplementedError("has_new_data à implémenter")


def build_fact_for_day(ds: str) -> pd.DataFrame:
    # TODO : construire les faits du jour ds (commandes 'completed' uniquement).
    #   - joindre order_items x orders(jour) x dim_product(unit_cost)
    #   - SCD2 : retrouver la version client valide à ds (valid_from <= ds <= valid_to)
    #            -> récupérer customer_sk et segment_at_order
    #   - retours : qty_returned agrégée par order_item_id (cap à quantity)
    #   - calculer montant_brut, montant_net, montant_retourne,
    #     montant_net_de_retours, cout_total, marge
    #   Renvoyer le DataFrame des faits (colonnes : voir README/CORRIGE).
    raise NotImplementedError("build_fact_for_day à implémenter")


# [FOURNI] Chargement idempotent de la partition du jour + audit.
def load_fact_incremental(ds: str, **_):
    fct = build_fact_for_day(ds)
    con = sqlite3.connect(WAREHOUSE)
    try:
        con.execute("""CREATE TABLE IF NOT EXISTS fct_sales (
            order_item_id TEXT, order_id TEXT, customer_id TEXT, customer_sk INTEGER,
            segment_at_order TEXT, product_id TEXT, order_date TEXT, channel TEXT,
            quantity INTEGER, qty_returned INTEGER, discount_pct REAL, unit_price REAL,
            unit_cost REAL, montant_brut REAL, montant_net REAL, montant_retourne REAL,
            montant_net_de_retours REAL, cout_total REAL, marge REAL)""")
        con.execute("DELETE FROM fct_sales WHERE order_date = ?", (ds,))  # idempotence
        con.commit()
        n = 0
        if fct is not None and not fct.empty:
            fct.to_sql("fct_sales", con, if_exists="append", index=False)
            n = len(fct)
        con.execute("INSERT INTO audit_runs VALUES (?,?,?,?,?)",
                    (ds, "fct_sales", int(n), "success", datetime.utcnow().isoformat(timespec="seconds")))
        con.commit()
        print(f"[LOAD fct] {ds}: {n} lignes")
    finally:
        con.close()


# =========================================================================== #
# SÉANCE 4 — AGRÉGAT + QUALITÉ + SUPERVISION
# =========================================================================== #
def refresh_aggregate_mart(**_):
    # TODO : (re)construire la table sales_monthly à partir de fct_sales x dim_date :
    #        year_month, ca_net (= SUM montant_net_de_retours), marge, qte_vendue, qte_retournee.
    raise NotImplementedError("refresh_aggregate_mart à implémenter")


def quality_gate(**_):
    # TODO : contrôles BLOQUANTS (lever une exception si KO) :
    #        fct_sales non vide ; customer_sk non nul ; aucun produit orphelin ;
    #        aucun montant_net_de_retours négatif.
    raise NotImplementedError("quality_gate à implémenter")


# [FOURNI] Supervision (lecture seule).
def report_metrics(**_):
    con = sqlite3.connect(WAREHOUSE)
    try:
        print("=== Supervision ===")
        for t in ["dim_customer_scd", "dim_product", "dim_date", "fct_sales", "sales_monthly", "rejects"]:
            try:
                print(f"  {t:18s}: {con.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]}")
            except Exception:
                print(f"  {t:18s}: (absente)")
    finally:
        con.close()
