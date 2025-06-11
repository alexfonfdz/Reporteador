def UPSERT_FAMILIES(enterprise: str):
    return (
f"""
INSERT INTO family (name, id_admin, enterprise) VALUES (%s, %s, '{enterprise}')
ON DUPLICATE KEY
UPDATE name=VALUES(name);
"""
    )

def UPSERT_SUBFAMILIES(enterprise: str):
    return (
f"""
INSERT INTO subfamily (name, id_admin, enterprise) VALUES (%s, %s, '{enterprise}')
ON DUPLICATE KEY
UPDATE name=VALUES(name);
"""
    )

def UPSERT_BRANDS(enterprise: str): 
    return (
f"""
INSERT INTO brand (name, id_admin, enterprise) VALUES (%s, %s, '{enterprise}')
ON DUPLICATE KEY
UPDATE name=VALUES(name);
"""
    )

def UPSERT_CATALOGS():
    return(
"""
INSERT IGNORE INTO catalog (name) VALUES(%s);
"""
    )

def UPSERT_PRODUCTS(enterprise: str):
    return (
f"""
INSERT INTO product (id_admin, code, description, create_date, brand_id, family_id, subfamily_id, enterprise)
VALUES (%s, %s, %s, %s,
    (SELECT id FROM brand WHERE id_admin = %s AND enterprise = '{enterprise}'),
    (SELECT id FROM family WHERE id_admin = %s AND enterprise = '{enterprise}'),
    (SELECT id FROM subfamily WHERE id_admin = %s AND enterprise = '{enterprise}'),
    '{enterprise}'
)
ON DUPLICATE KEY UPDATE 
    description=VALUES(description), code=VALUES(code),
    brand_id=VALUES(brand_id), family_id=VALUES(family_id),
    subfamily_id=VALUES(subfamily_id);
"""
    )

def UPSERT_MOVEMENTS(enterprise: str):
    return (
f"""
INSERT IGNORE INTO movements (
    id_admin, movement_date, is_order, is_input, is_output, pending, folio, serie,
    aditional_folio, paid, payment_method, quantity, total_quantity, amount, iva,
    discount, amount_discount, total, cost_of_sale, profit, currency, order_id_admin,
    movement_type, canceled, enterprise
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'{enterprise}');
"""
    )

def UPSERT_MOVEMENT_DETAILS(enterprise: str):
    return (
f"""
INSERT IGNORE INTO movements_detail (
    id_admin, movement_id, product_id, movement_detail_date, um, quantity, um_factor, unitary_price,
    total_quantity, amount, iva, discount, existence, cost_of_sale, canceled, enterprise
)
VALUES (
    %s, 
    (SELECT id from movements WHERE id_admin = %s AND enterprise = '{enterprise}'),
    (SELECT id from product WHERE id_admin = %s AND enterprise = '{enterprise}'),
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '{enterprise}'
);
"""
)



# Queries for ProductABC
def GET_DISTINCT_YEARS_MOVEMENTS(enterprise: str):
    return (
    f"""
SELECT DISTINCT YEAR(movement_date) AS year
FROM movements
WHERE enterprise = '{enterprise}';
"""
    )

def GET_TOTAL_AMOUNT_AND_TOTAL_PROFIT_BY_YEAR(enterprise: str):
    return (
    f"""
SELECT YEAR(movement_detail_date) AS year, SUM(amount) AS total_amount,
SUM(amount-(cost_of_sale * quantity)) AS total_profit
FROM movements_detail
WHERE enterprise = '{enterprise}' 
GROUP BY YEAR(movement_detail_date)
ORDER BY YEAR(movement_detail_date) DESC;
"""
    )

def GET_PRODUCTS_SALES_SUMMARY_BY_YEAR(enterprise: str, year: int):
    return (
    f"""
SELECT
    p.id AS product_id,
    p.code,
    p.description,
    p.create_date,
    CASE WHEN SUM(CASE WHEN YEAR(md.movement_detail_date) = {year} THEN md.amount ELSE 0 END) = 0 THEN NULL
         ELSE SUM(CASE WHEN YEAR(md.movement_detail_date) = {year} THEN md.amount ELSE 0 END)
    END AS total_amount,
    CASE WHEN SUM(CASE WHEN YEAR(md.movement_detail_date) = {year} THEN (md.amount - (md.cost_of_sale * md.quantity)) ELSE 0 END) = 0 THEN NULL
         ELSE SUM(CASE WHEN YEAR(md.movement_detail_date) = {year} THEN (md.amount - (md.cost_of_sale * md.quantity)) ELSE 0 END)
    END AS total_profit,
    CASE WHEN SUM(CASE WHEN YEAR(md.movement_detail_date) = {year} THEN md.quantity ELSE 0 END) = 0 THEN NULL
         ELSE SUM(CASE WHEN YEAR(md.movement_detail_date) = {year} THEN md.quantity ELSE 0 END)
    END AS total_quantity
FROM product p
LEFT JOIN movements_detail md
    ON md.product_id = p.id
    AND md.enterprise = '{enterprise}'
    AND YEAR(md.movement_detail_date) = {year}
WHERE p.enterprise = '{enterprise}'
  AND YEAR(p.create_date) <= {year}
GROUP BY p.id, p.code, p.description, p.create_date
ORDER BY
    total_amount DESC,
    CASE WHEN total_amount IS NULL THEN p.description ELSE NULL END ASC
;
"""
    )

def UPSERT_PRODUCT_ABC():
    return (
    f"""
    INSERT INTO product_abc (
        product_id, sales_percentage, acc_sales_percentage, sold_abc,
        profit_percentage, acc_profit_percentage, profit_abc, top_products,
        enterprise, year, last_update
    )
    VALUES (
        %(product_id)s, %(sales_percentage)s, %(acc_sales_percentage)s, %(sold_abc)s,
        %(profit_percentage)s, %(acc_profit_percentage)s, %(profit_abc)s, %(top_products)s,
        %(enterprise)s, %(year)s, %(last_update)s
    )
    ON DUPLICATE KEY UPDATE
        sales_percentage=VALUES(sales_percentage),
        acc_sales_percentage=VALUES(acc_sales_percentage),
        sold_abc=VALUES(sold_abc),
        profit_percentage=VALUES(profit_percentage),
        acc_profit_percentage=VALUES(acc_profit_percentage),
        profit_abc=VALUES(profit_abc),
        top_products=VALUES(top_products),
        last_update=VALUES(last_update)
    ;
    """
    )

def GET_DISTINCT_YEARS_MOVEMENTS_ALL():
    return (
    f"""
SELECT DISTINCT YEAR(movement_date) AS year
FROM movements;
"""
    )

def GET_TOTAL_AMOUNT_AND_TOTAL_PROFIT_BY_YEAR_ALL():
    return (
    f"""
SELECT YEAR(movement_detail_date) AS year, SUM(amount) AS total_amount,
SUM(amount-(cost_of_sale * quantity)) AS total_profit
FROM movements_detail
GROUP BY YEAR(movement_detail_date)
ORDER BY YEAR(movement_detail_date) DESC;
"""
    )

def GET_PRODUCTS_SALES_SUMMARY_BY_YEAR_ALL(year: int):
    return (
    f"""
SELECT
    p.id AS product_id,
    p.code,
    p.description,
    p.create_date,
    CASE WHEN SUM(CASE WHEN YEAR(md.movement_detail_date) = {year} THEN md.amount ELSE 0 END) = 0 THEN NULL
         ELSE SUM(CASE WHEN YEAR(md.movement_detail_date) = {year} THEN md.amount ELSE 0 END)
    END AS total_amount,
    CASE WHEN SUM(CASE WHEN YEAR(md.movement_detail_date) = {year} THEN (md.amount - (md.cost_of_sale * md.quantity)) ELSE 0 END) = 0 THEN NULL
         ELSE SUM(CASE WHEN YEAR(md.movement_detail_date) = {year} THEN (md.amount - (md.cost_of_sale * md.quantity)) ELSE 0 END)
    END AS total_profit,
    CASE WHEN SUM(CASE WHEN YEAR(md.movement_detail_date) = {year} THEN md.quantity ELSE 0 END) = 0 THEN NULL
         ELSE SUM(CASE WHEN YEAR(md.movement_detail_date) = {year} THEN md.quantity ELSE 0 END)
    END AS total_quantity
FROM product p
LEFT JOIN movements_detail md
    ON md.product_id = p.id
    AND YEAR(md.movement_detail_date) = {year}
WHERE YEAR(p.create_date) <= {year}
GROUP BY p.id, p.code, p.description, p.create_date
ORDER BY
    total_amount DESC,
    CASE WHEN total_amount IS NULL THEN p.description ELSE NULL END ASC
;
"""
    )