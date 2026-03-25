/* Ručna provjera unosa podataka u bazu podataka
Output selecta mora u potpunosti odgovarati csv podacima iz datoteke
*/

use dw;
SELECT DATABASE();
SHOW TABLES;

SELECT 
    cy.name AS retailer_country,
    od.name AS order_method_type,
    re.name AS retailer_type,
    pl.name AS product_line,
    pe.name AS product_type,
    pt.name AS product,
    ss.year,
    ss.quarter,
    ss.revenue,
    ss.quantity,
    ss.gross_margin
FROM sales ss
INNER JOIN country cy ON ss.country_fk = cy.id
INNER JOIN order_method od ON ss.order_method_fk = od.id
INNER JOIN retailer_type re ON ss.retailer_type_fk = re.id
INNER JOIN product pt ON ss.product_fk = pt.id
INNER JOIN product_type pe ON pt.product_type_fk = pe.id
INNER JOIN product_line pl ON pe.product_line_fk = pl.id
ORDER BY ss.id ASC
LIMIT 100;
