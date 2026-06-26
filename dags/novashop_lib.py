"""
novashop_lib.py — IMPLEMENTATION COMPLETE
Module B3 IA & Data — Outils ETL — Projet NovaShop (seances 3 & 4)
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta

import pandas as pd

BASE_DIR  = os.environ.get("ETL_BASE_DIR", "/opt/airflow")
DATA_DIR  = os.path.join(BASE_DIR, "data")
OUT_DIR   = os.path.join(BASE_DIR, "output")
STAGING   = os.path.join(OUT_DIR, "staging")
WAREHOUSE = os.path.join(OUT_DIR, "warehouse.db")
REJECTS   = os.path.join(STAGING, "rejects.csv")

SOURCES = {
    "customers":        "customers_raw.csv",
    "customer_changes": "customer_changes_raw.csv",
    "products":         "products_raw.csv",
    "orders":           "orders_raw.csv",
    "order_items":      "order_items_raw.csv",
    "returns":          "returns_raw.csv",
}

os.makedirs(STAGING, exist_ok=True)


def _stg(name: str) -> str:
    return os.path.join(STAGING, f"{name}.csv")


# ---------------------------------------------------------------------------
# [FOURNI] Helper quarantaine
# ---------------------------------------------------------------------------
def _reject(source: str, df_bad: pd.DataFrame, key_col: str, reason: str):
    if df_bad is None or df_bad.empty:
        return
    rec = pd.DataFrame({
        "source":       source,
        "business_key": df_bad[key_col].astype(str) if key_col in df_bad else "?",
        "reason":       reason,
        "rejected_at":  datetime.utcnow().isoformat(timespec="seconds"),
    })
    rec.to_csv(REJECTS, mode="a", header=not os.path.exists(REJECTS), index=False)
    print(f"[REJECT] {source}: {len(df_bad)} lignes -> {repr(reason)}")


# ---------------------------------------------------------------------------
# [FOURNI] EXTRACT
# ---------------------------------------------------------------------------
def extract_all(**_):
    if os.path.exists(REJECTS):
        os.remove(REJECTS)
    for name, fname in SOURCES.items():
        path = os.path.join(DATA_DIR, fname)
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        pd.read_csv(path, dtype=str).to_csv(_stg(f"{name}_raw"), index=False)
        print(f"[EXTRACT] {name} charge en staging")


# ===========================================================================
# SEANCE 3 — NETTOYAGE
# ===========================================================================

def _parse_date_iso(val: str, dayfirst: bool = False):
    try:
        return pd.to_datetime(val, dayfirst=dayfirst).strftime("%Y-%m-%d")
    except Exception:
        return None


def clean_customers(**_):
    df = pd.read_csv(_stg("customers_raw"), dtype=str)

    # Doublons
    dupes = df[df.duplicated(subset="customer_id", keep="first")]
    _reject("customers", dupes, "customer_id", "doublon customer_id")
    df = df.drop_duplicates(subset="customer_id", keep="first")

    # Normalisation casse
    for col in ["full_name", "country", "segment"]:
        df[col] = df[col].str.strip().str.title()

    # Email manquant
    df["email_manquant"] = df["email"].isna() | (df["email"].str.strip() == "")
    df.loc[df["email_manquant"], "email"] = pd.NA

    # Date FR -> ISO
    df["signup_date"] = df["signup_date"].apply(lambda v: _parse_date_iso(v, dayfirst=True))
    bad_dates = df[df["signup_date"].isna()]
    _reject("customers", bad_dates, "customer_id", "signup_date invalide")
    df = df.dropna(subset=["signup_date"])

    df.to_csv(_stg("stg_customers"), index=False)
    print(f"[CLEAN] customers: {len(df)} lignes OK")


def clean_customer_changes(**_):
    df = pd.read_csv(_stg("customer_changes_raw"), dtype=str)

    df["change_date"] = df["change_date"].apply(lambda v: _parse_date_iso(v))
    bad = df[df["change_date"].isna()]
    _reject("customer_changes", bad, "customer_id", "change_date invalide")
    df = df.dropna(subset=["change_date"])

    df["new_segment"] = df["new_segment"].str.strip().str.title()

    df.to_csv(_stg("stg_customer_changes"), index=False)
    print(f"[CLEAN] customer_changes: {len(df)} lignes OK")


def clean_products(**_):
    df = pd.read_csv(_stg("products_raw"), dtype=str)

    df["unit_cost"]  = pd.to_numeric(df["unit_cost"],  errors="coerce")
    df["list_price"] = pd.to_numeric(df["list_price"], errors="coerce")

    no_price = df[df["list_price"].isna()]
    _reject("products", no_price, "product_id", "list_price manquant")
    df = df.dropna(subset=["list_price"])

    df["category"] = df["category"].str.strip()
    df.loc[df["category"].isna() | (df["category"] == ""), "category"] = "Non categorise"

    df.to_csv(_stg("stg_products"), index=False)
    print(f"[CLEAN] products: {len(df)} lignes OK")


def clean_orders(**_):
    df = pd.read_csv(_stg("orders_raw"), dtype=str)

    bad = df[df["order_id"].isna() | (df["order_id"].str.strip() == "")]
    _reject("orders", bad, "order_id", "order_id nul")
    df = df.dropna(subset=["order_id"])
    df = df[df["order_id"].str.strip() != ""].copy()

    df["order_date"] = df["order_date"].apply(lambda v: _parse_date_iso(v))
    bad_dates = df[df["order_date"].isna()]
    _reject("orders", bad_dates, "order_id", "order_date invalide")
    df = df.dropna(subset=["order_date"])

    df["status"]  = df["status"].str.strip().str.lower()
    df["channel"] = df["channel"].str.strip().str.lower()

    df.to_csv(_stg("stg_orders"), index=False)
    print(f"[CLEAN] orders: {len(df)} lignes OK")


def clean_order_items(**_):
    df       = pd.read_csv(_stg("order_items_raw"), dtype=str)
    products = pd.read_csv(_stg("stg_products"))

    dupes = df[df.duplicated(subset="order_item_id", keep="first")]
    _reject("order_items", dupes, "order_item_id", "doublon order_item_id")
    df = df.drop_duplicates(subset="order_item_id", keep="first")

    df["quantity"]     = pd.to_numeric(df["quantity"],     errors="coerce")
    df["discount_pct"] = pd.to_numeric(df["discount_pct"], errors="coerce").fillna(0.0)
    df["unit_price"]   = pd.to_numeric(df["unit_price"],   errors="coerce")

    bad_qty = df[df["quantity"].isna() | (df["quantity"] <= 0)]
    _reject("order_items", bad_qty, "order_item_id", "quantity invalide (<= 0)")
    df = df[df["quantity"].notna() & (df["quantity"] > 0)].copy()

    valid_products = set(products["product_id"].astype(str))
    orphans = df[~df["product_id"].isin(valid_products)]
    _reject("order_items", orphans, "order_item_id", "product_id orphelin")
    df = df[df["product_id"].isin(valid_products)].copy()

    price_map = products.set_index("product_id")["list_price"].to_dict()
    mask = df["unit_price"].isna()
    df.loc[mask, "unit_price"] = df.loc[mask, "product_id"].map(price_map)

    df.to_csv(_stg("stg_order_items"), index=False)
    print(f"[CLEAN] order_items: {len(df)} lignes OK")


def clean_returns(**_):
    df          = pd.read_csv(_stg("returns_raw"), dtype=str)
    order_items = pd.read_csv(_stg("stg_order_items"))

    dupes = df[df.duplicated(subset="return_id", keep="first")]
    _reject("returns", dupes, "return_id", "doublon return_id")
    df = df.drop_duplicates(subset="return_id", keep="first")

    df["qty_returned"] = pd.to_numeric(df["qty_returned"], errors="coerce")
    bad_qty = df[df["qty_returned"].isna()]
    _reject("returns", bad_qty, "return_id", "qty_returned non numerique")
    df = df.dropna(subset=["qty_returned"])

    valid_items = set(order_items["order_item_id"].astype(str))
    orphans = df[~df["order_item_id"].isin(valid_items)]
    _reject("returns", orphans, "return_id", "order_item_id orphelin")
    df = df[df["order_item_id"].isin(valid_items)].copy()

    df["return_date"] = df["return_date"].apply(lambda v: _parse_date_iso(v))
    bad_dates = df[df["return_date"].isna()]
    _reject("returns", bad_dates, "return_id", "return_date invalide")
    df = df.dropna(subset=["return_date"])

    df.to_csv(_stg("stg_returns"), index=False)
    print(f"[CLEAN] returns: {len(df)} lignes OK")


# ===========================================================================
# SEANCE 3/4 — SCD TYPE 2
# ===========================================================================

def build_dim_customer_scd2(**_):
    customers = pd.read_csv(_stg("stg_customers"))
    changes   = pd.read_csv(_stg("stg_customer_changes"))

    records = []
    for _, cust in customers.iterrows():
        cid = cust["customer_id"]
        cust_changes = (
            changes[changes["customer_id"] == cid]
            .sort_values("change_date")
            .reset_index(drop=True)
        )
        # Liste des versions (segment, valid_from)
        versions = [(cust["segment"], "1900-01-01")]
        for _, chg in cust_changes.iterrows():
            versions.append((chg["new_segment"], chg["change_date"]))

        for i, (segment, valid_from) in enumerate(versions):
            is_last = (i == len(versions) - 1)
            if is_last:
                valid_to   = "9999-12-31"
                is_current = 1
            else:
                next_date = pd.to_datetime(versions[i + 1][1])
                valid_to  = (next_date - timedelta(days=1)).strftime("%Y-%m-%d")
                is_current = 0

            records.append({
                "customer_id": cid,
                "full_name":   cust.get("full_name"),
                "email":       cust.get("email"),
                "country":     cust.get("country"),
                "signup_date": cust.get("signup_date"),
                "segment":     segment,
                "valid_from":  valid_from,
                "valid_to":    valid_to,
                "is_current":  is_current,
            })

    dim = pd.DataFrame(records).reset_index(drop=True)
    dim.insert(0, "customer_sk", dim.index + 1)

    dim.to_csv(_stg("dim_customer_scd"), index=False)
    print(f"[SCD2] dim_customer_scd: {len(dim)} versions pour {dim['customer_id'].nunique()} clients")


# ===========================================================================
# SEANCE 4 — DIMENSIONS SIMPLES
# ===========================================================================

def build_dimensions(**_):
    # dim_product
    products = pd.read_csv(_stg("stg_products"))
    products.to_csv(_stg("dim_product"), index=False)
    print(f"[DIM] dim_product: {len(products)} produits")

    # dim_date
    orders = pd.read_csv(_stg("stg_orders"))
    orders["order_date"] = pd.to_datetime(orders["order_date"])
    date_range = pd.date_range(orders["order_date"].min(), orders["order_date"].max(), freq="D")

    dim_date = pd.DataFrame({"order_date": date_range.strftime("%Y-%m-%d")})
    dt       = pd.to_datetime(dim_date["order_date"])
    dim_date["year"]       = dt.dt.year
    dim_date["month"]      = dt.dt.month
    dim_date["year_month"] = dt.dt.strftime("%Y-%m")
    dim_date["quarter"]    = dt.dt.quarter

    dim_date.to_csv(_stg("dim_date"), index=False)
    print(f"[DIM] dim_date: {len(dim_date)} jours")


# ===========================================================================
# [FOURNI] CHARGEMENT DES TABLES DE REFERENCE
# ===========================================================================

def load_reference_tables(**_):
    con = sqlite3.connect(WAREHOUSE)
    try:
        for t in ["dim_product", "dim_date", "dim_customer_scd"]:
            pd.read_csv(_stg(t)).to_sql(t, con, if_exists="replace", index=False)
        rej = (pd.read_csv(REJECTS) if os.path.exists(REJECTS)
               else pd.DataFrame(columns=["source", "business_key", "reason", "rejected_at"]))
        rej.to_sql("rejects", con, if_exists="replace", index=False)
        con.execute(
            "CREATE TABLE IF NOT EXISTS audit_runs "
            "(run_ds TEXT, table_name TEXT, rows_loaded INTEGER, status TEXT, logged_at TEXT)"
        )
        con.commit()
        print(f"[LOAD ref] dims + rejects ({len(rej)} en quarantaine)")
    finally:
        con.close()


# ===========================================================================
# SEANCE 4 — INCREMENTAL PAR JOUR
# ===========================================================================

def has_new_data(ds: str) -> bool:
    assert ds is not None, "ds ne peut pas etre None"
    orders = pd.read_csv(_stg("stg_orders"))
    return not orders[
        (orders["order_date"] == ds) & (orders["status"] == "completed")
    ].empty


def build_fact_for_day(ds: str) -> pd.DataFrame:
    assert ds is not None, "ds ne peut pas etre None"

    orders      = pd.read_csv(_stg("stg_orders"))
    order_items = pd.read_csv(_stg("stg_order_items"))
    products    = pd.read_csv(_stg("stg_products"))
    dim_cust    = pd.read_csv(_stg("dim_customer_scd"))
    stg_ret     = _stg("stg_returns")
    returns     = pd.read_csv(stg_ret) if os.path.exists(stg_ret) else pd.DataFrame()

    # Commandes completed du jour
    day_orders = orders[(orders["order_date"] == ds) & (orders["status"] == "completed")]
    if day_orders.empty:
        return pd.DataFrame()

    # order_items x orders du jour
    df = order_items.merge(
        day_orders[["order_id", "customer_id", "order_date", "channel"]],
        on="order_id"
    )

    # Joindre unit_cost depuis dim_product
    df = df.merge(products[["product_id", "unit_cost"]], on="product_id", how="left")

    # Jointure SCD2 : version valide a ds
    dim_day = dim_cust[(dim_cust["valid_from"] <= ds) & (dim_cust["valid_to"] >= ds)]
    df = df.merge(
        dim_day[["customer_id", "customer_sk", "segment"]],
        on="customer_id", how="left"
    )
    df.rename(columns={"segment": "segment_at_order"}, inplace=True)

    # Agreger retours par order_item_id
    if not returns.empty:
        ret_agg = returns.groupby("order_item_id")["qty_returned"].sum().reset_index()
        df = df.merge(ret_agg, on="order_item_id", how="left")
    else:
        df["qty_returned"] = 0

    df["qty_returned"] = df["qty_returned"].fillna(0).astype(int)

    # Cast numerique
    df["quantity"]     = df["quantity"].astype(float)
    df["unit_price"]   = df["unit_price"].astype(float)
    df["unit_cost"]    = df["unit_cost"].fillna(0).astype(float)
    df["discount_pct"] = df["discount_pct"].fillna(0).astype(float)

    # Cap retours a la quantite commandee
    df["qty_returned"] = df[["qty_returned", "quantity"]].min(axis=1).astype(int)

    # Metriques financieres
    df["montant_brut"]           = df["quantity"] * df["unit_price"]
    df["montant_net"]            = df["montant_brut"] * (1 - df["discount_pct"])
    df["montant_retourne"]       = df["qty_returned"] * df["unit_price"]
    df["montant_net_de_retours"] = df["montant_net"] - df["montant_retourne"]
    df["cout_total"]             = df["quantity"] * df["unit_cost"]
    df["marge"]                  = df["montant_net_de_retours"] - df["cout_total"]

    cols = [
        "order_item_id", "order_id", "customer_id", "customer_sk",
        "segment_at_order", "product_id", "order_date", "channel",
        "quantity", "qty_returned", "discount_pct", "unit_price", "unit_cost",
        "montant_brut", "montant_net", "montant_retourne",
        "montant_net_de_retours", "cout_total", "marge",
    ]
    return df[[c for c in cols if c in df.columns]]


# [FOURNI] Chargement idempotent
def load_fact_incremental(ds: str, **_):
    fct = build_fact_for_day(ds)
    con = sqlite3.connect(WAREHOUSE)
    try:
        con.execute(
            "CREATE TABLE IF NOT EXISTS fct_sales ("
            "order_item_id TEXT, order_id TEXT, customer_id TEXT, customer_sk INTEGER, "
            "segment_at_order TEXT, product_id TEXT, order_date TEXT, channel TEXT, "
            "quantity INTEGER, qty_returned INTEGER, discount_pct REAL, unit_price REAL, "
            "unit_cost REAL, montant_brut REAL, montant_net REAL, montant_retourne REAL, "
            "montant_net_de_retours REAL, cout_total REAL, marge REAL)"
        )
        con.execute("DELETE FROM fct_sales WHERE order_date = ?", (ds,))
        con.commit()
        n = 0
        if fct is not None and not fct.empty:
            fct.to_sql("fct_sales", con, if_exists="append", index=False)
            n = len(fct)
        con.execute(
            "INSERT INTO audit_runs VALUES (?,?,?,?,?)",
            (ds, "fct_sales", int(n), "success", datetime.utcnow().isoformat(timespec="seconds"))
        )
        con.commit()
        print(f"[LOAD fct] {ds}: {n} lignes")
    finally:
        con.close()


# ===========================================================================
# SEANCE 4 — AGREGAT + QUALITE + SUPERVISION
# ===========================================================================

def refresh_aggregate_mart(**_):
    con = sqlite3.connect(WAREHOUSE)
    try:
        exists = pd.read_sql(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='fct_sales'",
            con
        )
        if exists.empty:
            pd.DataFrame(columns=["year_month", "ca_net", "marge", "qte_vendue", "qte_retournee"])\
              .to_sql("sales_monthly", con, if_exists="replace", index=False)
            print("[MART] fct_sales absente -> sales_monthly vide")
            return

        fct = pd.read_sql("SELECT * FROM fct_sales", con)
        if fct.empty:
            pd.DataFrame(columns=["year_month", "ca_net", "marge", "qte_vendue", "qte_retournee"])\
              .to_sql("sales_monthly", con, if_exists="replace", index=False)
            print("[MART] fct_sales vide -> sales_monthly vide")
            return

        dim_date = pd.read_sql("SELECT order_date, year_month FROM dim_date", con)
        df = fct.merge(dim_date, on="order_date", how="left")
        mart = df.groupby("year_month").agg(
            ca_net=("montant_net_de_retours", "sum"),
            marge=("marge", "sum"),
            qte_vendue=("quantity", "sum"),
            qte_retournee=("qty_returned", "sum"),
        ).reset_index()

        mart.to_sql("sales_monthly", con, if_exists="replace", index=False)
        con.commit()
        print(f"[MART] sales_monthly: {len(mart)} mois")
    finally:
        con.close()


def quality_gate(**_):
    con = sqlite3.connect(WAREHOUSE)
    try:
        exists = pd.read_sql(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='fct_sales'",
            con
        )
        if exists.empty:
            raise ValueError("[QUALITY] ECHEC : fct_sales absente")

        fct = pd.read_sql("SELECT * FROM fct_sales", con)

        if fct.empty:
            raise ValueError("[QUALITY] ECHEC : fct_sales est vide")

        if fct["customer_sk"].isna().any():
            raise ValueError("[QUALITY] ECHEC : customer_sk nul")

        products = pd.read_sql("SELECT product_id FROM dim_product", con)
        if (~fct["product_id"].isin(products["product_id"])).any():
            raise ValueError("[QUALITY] ECHEC : produits orphelins")

        if (fct["montant_net_de_retours"] < 0).any():
            raise ValueError("[QUALITY] ECHEC : montant_net_de_retours negatif")

        print("[QUALITY] Tous les controles OK")
    finally:
        con.close()


# [FOURNI] Supervision
def report_metrics(**_):
    con = sqlite3.connect(WAREHOUSE)
    try:
        print("=== Supervision ===")
        for t in ["dim_customer_scd", "dim_product", "dim_date",
                  "fct_sales", "sales_monthly", "rejects"]:
            try:
                n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                print(f"  {t:20s}: {n}")
            except Exception:
                print(f"  {t:20s}: (absente)")
    finally:
        con.close()
