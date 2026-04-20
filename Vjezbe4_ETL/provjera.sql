/*******************************************************************************
  SQL SKRIPTA ZA ANALIZU DIMENZIJSKOG MODELA (STAR SCHEMA)
  Ovi upiti demonstriraju analizu podataka nakon uspješnog ETL procesa.
*******************************************************************************/

-- 1. Postavljanje baze podataka
USE dw;

-- =============================================================================
-- DIO 1: VERIFIKACIJA PODATAKA
-- Provjeravamo jesu li sve tablice napunjene i imaju li surogatne ključeve
-- =============================================================================

SELECT 'dim_retailer' as Tablica, COUNT(*) as Broj_Redova FROM dim_retailer
UNION ALL
SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL
SELECT 'dim_country', COUNT(*) FROM dim_country
UNION ALL
SELECT 'dim_order_method', COUNT(*) FROM dim_order_method
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL
SELECT 'fact_sales', COUNT(*) FROM fact_sales;


-- DIO 2: (BUSINESS INTELLIGENCE)

-- IZVJEŠTAJ 1: Top 5 država po ukupnom prihodu
SELECT 
    c.name AS Drzava, 
    c.region AS Regija,
    ROUND(SUM(f.revenue), 2) AS Ukupni_Prihod,
    SUM(f.quantity) AS Prodana_Kolicina
FROM fact_sales f
JOIN dim_country c ON f.country_tk = c.country_tk
GROUP BY c.name, c.region
ORDER BY Ukupni_Prihod DESC
LIMIT 5;


-- IZVJEŠTAJ 2: Analiza profitabilnosti po linijama proizvoda
SELECT 
    p.product_line_name AS Kategorija_Proizvoda,
    ROUND(SUM(f.revenue), 2) AS Prihod,
    ROUND(AVG(f.gross_margin), 4) AS Prosjecna_Marza
FROM fact_sales f
JOIN dim_product p ON f.product_tk = p.product_tk
GROUP BY p.product_line_name
ORDER BY Prosjecna_Marza DESC;


-- IZVJEŠTAJ 3: Trend prodaje kroz vrijeme (Godina/Kvartal)
SELECT 
    d.year AS Godina,
    d.quarter AS Kvartal,
    ROUND(SUM(f.revenue), 2) AS Prihod
FROM fact_sales f
JOIN dim_date d ON f.date_tk = d.date_tk
GROUP BY d.year, d.quarter
ORDER BY d.year DESC, d.quarter DESC;


-- IZVJEŠTAJ 4: Najčešće metode narudžbe po tipu trgovine
SELECT 
    om.name AS Metoda_Narudzbe,
    r.name AS Trgovina,
    COUNT(f.fact_sales_tk) AS Broj_Transakcija,
    ROUND(SUM(f.revenue), 2) AS Vrijednost_Prodaje
FROM fact_sales f
JOIN dim_order_method om ON f.order_method_tk = om.order_method_tk
JOIN dim_retailer r ON f.retailer_tk = r.retailer_tk
GROUP BY om.name, r.name
ORDER BY Vrijednost_Prodaje DESC
LIMIT 10;