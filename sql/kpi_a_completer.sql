-- =============================================================================
-- EXERCICE — 3 KPI à calculer sur l'entrepôt NovaShop (output/warehouse.db)
-- Tables : fct_sales, dim_customer_scd (SCD2), dim_product, dim_date,
--          sales_monthly (agrégat), rejects (quarantaine), audit_runs.
-- Outil  : DB Browser for SQLite ou `sqlite3 output/warehouse.db`.
-- =============================================================================

-- KPI 1 — CA NET DE RETOURS mensuel (+ variation % vs mois précédent)
-- Attendu : year_month, ca_net, variation_pct (fonction de fenêtrage LAG).
-- Astuce  : l'agrégat sales_monthly contient déjà le ca_net par mois.
-- SELECT ... ;

SELECT
    year_month,
    ROUND(ca_net, 2)                                          AS ca_net,
    ROUND(marge, 2)                                           AS marge,
    ROUND(marge / NULLIF(ca_net, 0) * 100, 1)                AS taux_marge_pct,
    ROUND(
        (ca_net - LAG(ca_net) OVER (ORDER BY year_month))
        / NULLIF(LAG(ca_net) OVER (ORDER BY year_month), 0) * 100, 1
    )                                                         AS variation_pct
FROM sales_monthly
ORDER BY year_month;


-- KPI 2 — TAUX DE RETOUR par catégorie de produit
-- Attendu : category, taux_retour_pct = 100 * SUM(qty_returned)/SUM(quantity),
--           + CA net de retours. Identifier la catégorie la plus retournée.
-- Tables  : fct_sales × dim_product.
-- SELECT ... ;

SELECT
    p.category,
    SUM(f.quantity)                                            AS qte_vendue,
    SUM(f.qty_returned)                                        AS qte_retournee,
    ROUND(SUM(f.qty_returned) * 100.0
          / NULLIF(SUM(f.quantity), 0), 2)                    AS taux_retour_pct
FROM fct_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.category
ORDER BY taux_retour_pct DESC;



-- KPI 3 — PANIER MOYEN par segment AU MOMENT DE LA COMMANDE (SCD Type 2)
-- Attendu : utiliser fct_sales.segment_at_order (segment historisé valide à la
--           date de commande), pas le segment courant du client.
--           segment, panier_moyen (CA net moyen / commande), nb_commandes.
-- SELECT ... ;

SELECT
    segment_at_order,
    COUNT(DISTINCT order_id)                                   AS nb_commandes,
    ROUND(SUM(montant_net_de_retours), 2)                      AS ca_net_total,
    ROUND(SUM(montant_net_de_retours)
          / NULLIF(COUNT(DISTINCT order_id), 0), 2)           AS panier_moyen
FROM fct_sales
GROUP BY segment_at_order
ORDER BY panier_moyen DESC;

