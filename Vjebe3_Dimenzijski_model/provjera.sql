-- Koristi dw bazu
USE dw;

-- Pogledaj sve tablice
SHOW TABLES;

-- Provjeri broj redaka
SELECT 'dim_retailer' AS tabela, COUNT(*) AS broj FROM dim_retailer
UNION ALL SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL SELECT 'dim_country', COUNT(*) FROM dim_country
UNION ALL SELECT 'dim_order_method', COUNT(*) FROM dim_order_method
UNION ALL SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL SELECT 'fact_sales', COUNT(*) FROM fact_sales;

-- Isprobavanje analitičkih upita

-- Ukupna prodaja po godinama
SELECT 
    d.year,
    SUM(f.revenue) AS ukupna_prodaja,
    SUM(f.quantity) AS ukupna_kolicina,
    COUNT(*) AS broj_transakcija
FROM fact_sales f
INNER JOIN dim_date d ON f.date_tk = d.date_tk
GROUP BY d.year
ORDER BY d.year;

--Prodaja po državama
SELECT 
    c.name AS drzava,
    c.region AS regija,
    SUM(f.revenue) AS ukupna_prodaja,
    ROUND(AVG(f.revenue), 2) AS prosjecna_prodaja
FROM fact_sales f
INNER JOIN dim_country c ON f.country_tk = c.country_tk
GROUP BY c.name, c.region
ORDER BY ukupna_prodaja DESC
LIMIT 10;

--Najprodavaniji proizvodi
SELECT 
    p.product_name AS proizvod,
    p.product_line_name AS linija,
    SUM(f.quantity) AS ukupna_kolicina,
    SUM(f.revenue) AS ukupna_prodaja
FROM fact_sales f
INNER JOIN dim_product p ON f.product_tk = p.product_tk
GROUP BY p.product_name, p.product_line_name
ORDER BY ukupna_prodaja DESC
LIMIT 10;

--Prodaja po načinu narudžbe
SELECT 
    om.name AS nacin_narudzbe,
    COUNT(*) AS broj_narudzbi,
    SUM(f.revenue) AS ukupna_prodaja,
    ROUND(AVG(f.revenue), 2) AS prosjecna_vrijednost
FROM fact_sales f
INNER JOIN dim_order_method om ON f.order_method_tk = om.order_method_tk
GROUP BY om.name
ORDER BY ukupna_prodaja DESC;

-- Analiza marže po linijama proizvoda
SELECT 
    p.product_line_name AS linija_proizvoda,
    ROUND(AVG(f.gross_margin) * 100, 2) AS prosjecna_margin_postotak,
    SUM(f.revenue) AS ukupna_prodaja
FROM fact_sales f
INNER JOIN dim_product p ON f.product_tk = p.product_tk
GROUP BY p.product_line_name
ORDER BY prosjecna_margin_postotak DESC;

-- Kreiraj view koji spaja sve dimenzije
CREATE VIEW v_sales_analytics AS
SELECT 
    c.name AS country,
    c.region,
    r.name AS retailer_type,
    p.product_name,
    p.product_type_name,
    p.product_line_name,
    om.name AS order_method,
    d.year,
    d.quarter,
    f.revenue,
    f.quantity,
    f.gross_margin,
    (f.revenue * f.gross_margin) AS margin_amount
FROM fact_sales f
INNER JOIN dim_country c ON f.country_tk = c.country_tk
INNER JOIN dim_retailer r ON f.retailer_tk = r.retailer_tk
INNER JOIN dim_product p ON f.product_tk = p.product_tk
INNER JOIN dim_order_method om ON f.order_method_tk = om.order_method_tk
INNER JOIN dim_date d ON f.date_tk = d.date_tk;

-- Sada možete jednostavno raditi upite na view-u
SELECT * FROM v_sales_analytics LIMIT 10;

-- Agregacije preko view-a
SELECT 
    country,
    year,
    SUM(revenue) AS total_revenue
FROM v_sales_analytics
GROUP BY country, year
ORDER BY country, year;

-- Eksport za Power BI / Excel
SELECT 
    c.name AS Drzava,
    c.region AS Regija,
    p.product_line_name AS LinijaProizvoda,
    d.year AS Godina,
    d.quarter AS Kvartal,
    SUM(f.revenue) AS UkupnaProdaja,
    SUM(f.quantity) AS UkupnaKolicina,
    AVG(f.gross_margin) * 100 AS ProsjecnaMarza
FROM fact_sales f
INNER JOIN dim_country c ON f.country_tk = c.country_tk
INNER JOIN dim_product p ON f.product_tk = p.product_tk
INNER JOIN dim_date d ON f.date_tk = d.date_tk
GROUP BY c.name, c.region, p.product_line_name, d.year, d.quarter
ORDER BY c.name, d.year, d.quarter;

-- 1. Ukupna statistika
SELECT 
    'Ukupna prodaja' AS metrika, 
    ROUND(SUM(revenue), 2) AS vrijednost 
FROM fact_sales
UNION ALL
SELECT 'Ukupna količina', SUM(quantity) FROM fact_sales
UNION ALL
SELECT 'Broj transakcija', COUNT(*) FROM fact_sales
UNION ALL
SELECT 'Prosječna prodaja', ROUND(AVG(revenue), 2) FROM fact_sales;

-- 2. Rangiranje država
SELECT 
    c.name AS drzava,
    SUM(f.revenue) AS prodaja,
    RANK() OVER (ORDER BY SUM(f.revenue) DESC) AS rang
FROM fact_sales f
INNER JOIN dim_country c ON f.country_tk = c.country_tk
GROUP BY c.name;

-- 3. Rast prodaje po godinama
SELECT 
    year,
    SUM(revenue) AS prodaja,
    LAG(SUM(revenue)) OVER (ORDER BY year) AS prosla_godina,
    ROUND((SUM(revenue) - LAG(SUM(revenue)) OVER (ORDER BY year)) / 
          LAG(SUM(revenue)) OVER (ORDER BY year) * 100, 2) AS postotak_rasta
FROM fact_sales f
INNER JOIN dim_date d ON f.date_tk = d.date_tk
GROUP BY year;