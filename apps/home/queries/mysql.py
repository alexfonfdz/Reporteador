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
INSERT INTO movements (
    id_admin, movement_date, is_order, is_input, is_output, pending, folio, serie,
    aditional_folio, paid, payment_method, quantity, total_quantity, amount, iva,
    discount, amount_discount, total, cost_of_sale, profit, currency, order_id_admin,
    movement_type, canceled, enterprise
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'{enterprise}')
ON DUPLICATE KEY UPDATE
movement_date=VALUES(movement_date), is_order=VALUES(is_order), is_input=VALUES(is_input), is_output=VALUES(is_output),
pending=VALUES(pending), folio=VALUES(folio), serie=VALUES(serie),aditional_folio=VALUES(aditional_folio),
paid=VALUES(paid), payment_method=VALUES(payment_method), quantity=VALUES(quantity),
total_quantity=VALUES(total_quantity), amount=VALUES(amount), iva=VALUES(iva),discount=VALUES(discount),
amount_discount=VALUES(amount_discount), total=VALUES(total), cost_of_sale=VALUES(cost_of_sale),
profit=VALUES(profit), currency=VALUES(currency), order_id_admin=VALUES(order_id_admin),
movement_type=VALUES(movement_type), canceled=VALUES(canceled);
"""
    )

def UPSERT_MOVEMENT_DETAILS(enterprise: str):
    return (
f"""
INSERT INTO movements_detail (
    id_admin, movement_id, product_id, movement_detail_date, um, quantity, um_factor, unitary_price,
    total_quantity, amount, iva, discount, existence, cost_of_sale, canceled, enterprise
)
VALUES (
    %s, 
    (SELECT id from movements WHERE id_admin = %s AND enterprise = '{enterprise}'),
    (SELECT id from product WHERE id_admin = %s AND enterprise = '{enterprise}'),
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '{enterprise}'
)
ON DUPLICATE KEY UPDATE
movement_id=VALUES(movement_id), product_id=VALUES(product_id), movement_detail_date=VALUES(movement_detail_date),
um=VALUES(um), quantity=VALUES(quantity), um_factor=VALUES(um_factor), unitary_price=VALUES(unitary_price),
total_quantity=VALUES(total_quantity), amount=VALUES(amount), iva=VALUES(iva), discount=VALUES(discount),
existence=VALUES(existence), cost_of_sale=VALUES(cost_of_sale), canceled=VALUES(canceled);
"""
)

def INSERT_TABLE_UPDATE(enterprise: str):
    return (f"""
INSERT INTO table_update (table_name, affected_rows, enterprise, created_at)
VALUES (%s, %s, '{enterprise}', NOW())
""")

def GET_MOST_RECENT_UPDATE_UTC(enterprise: str):
    return (f"""
SELECT CONVERT_TZ(most_recent.update_date, @@global.time_zone, 'UTC') AS update_date 
FROM (
    SELECT MAX(created_at) AS update_date FROM table_update WHERE enterprise = '{enterprise}' AND table_name = %s
) AS most_recent;
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

def UPSERT_ANALYSIS_ABC():
    return (
    f"""
    INSERT INTO analysis_abc (
        catalog_id, family_id, subfamily_id, total_amount, profit, profit_percentage, units_sold, inventory_close_u, inventory_close_p, monthly_roi, sold_average_month, profit_average_month, actual_inventory, average_selling_cost, inventory_average_u, inventory_average_p, inventory_days,
        sales_percentage, acc_sales_percentage, sold_abc, acc_profit_percentage, profit_abc, top_products,
        month_sale_u_january, month_sale_p_january, inventory_close_u_january, inventory_close_p_january,
        month_sale_u_february, month_sale_p_february, inventory_close_u_february, inventory_close_p_february,
        month_sale_u_march, month_sale_p_march, inventory_close_u_march, inventory_close_p_march,
        month_sale_u_april, month_sale_p_april, inventory_close_u_april, inventory_close_p_april,
        month_sale_u_may, month_sale_p_may, inventory_close_u_may, inventory_close_p_may,
        month_sale_u_june, month_sale_p_june, inventory_close_u_june, inventory_close_p_june,
        month_sale_u_july, month_sale_p_july, inventory_close_u_july, inventory_close_p_july,
        month_sale_u_august, month_sale_p_august, inventory_close_u_august, inventory_close_p_august,
        month_sale_u_september, month_sale_p_september, inventory_close_u_september, inventory_close_p_september,
        month_sale_u_october, month_sale_p_october, inventory_close_u_october, inventory_close_p_october,
        month_sale_u_november, month_sale_p_november, inventory_close_u_november, inventory_close_p_november,
        month_sale_u_december, month_sale_p_december, inventory_close_u_december, inventory_close_p_december,
        enterprise, year, last_update
    )
    VALUES (
        %(catalog_id)s, %(family_id)s, %(subfamily_id)s, %(total_amount)s, %(profit)s, %(profit_percentage)s, %(units_sold)s, %(inventory_close_u)s, %(inventory_close_p)s, %(monthly_roi)s, %(sold_average_month)s, %(profit_average_month)s, %(actual_inventory)s, %(average_selling_cost)s, %(inventory_average_u)s, %(inventory_average_p)s, %(inventory_days)s,
        %(sales_percentage)s, %(acc_sales_percentage)s, %(sold_abc)s, %(acc_profit_percentage)s, %(profit_abc)s, %(top_products)s,
        %(month_sale_u_january)s, %(month_sale_p_january)s, %(inventory_close_u_january)s, %(inventory_close_p_january)s,
        %(month_sale_u_february)s, %(month_sale_p_february)s, %(inventory_close_u_february)s, %(inventory_close_p_february)s,
        %(month_sale_u_march)s, %(month_sale_p_march)s, %(inventory_close_u_march)s, %(inventory_close_p_march)s,
        %(month_sale_u_april)s, %(month_sale_p_april)s, %(inventory_close_u_april)s, %(inventory_close_p_april)s,
        %(month_sale_u_may)s, %(month_sale_p_may)s, %(inventory_close_u_may)s, %(inventory_close_p_may)s,
        %(month_sale_u_june)s, %(month_sale_p_june)s, %(inventory_close_u_june)s, %(inventory_close_p_june)s,
        %(month_sale_u_july)s, %(month_sale_p_july)s, %(inventory_close_u_july)s, %(inventory_close_p_july)s,
        %(month_sale_u_august)s, %(month_sale_p_august)s, %(inventory_close_u_august)s, %(inventory_close_p_august)s,
        %(month_sale_u_september)s, %(month_sale_p_september)s, %(inventory_close_u_september)s, %(inventory_close_p_september)s,
        %(month_sale_u_october)s, %(month_sale_p_october)s, %(inventory_close_u_october)s, %(inventory_close_p_october)s,
        %(month_sale_u_november)s, %(month_sale_p_november)s, %(inventory_close_u_november)s, %(inventory_close_p_november)s,
        %(month_sale_u_december)s, %(month_sale_p_december)s, %(inventory_close_u_december)s, %(inventory_close_p_december)s,
        %(enterprise)s, %(year)s, %(last_update)s
    )
    ON DUPLICATE KEY UPDATE
        total_amount=VALUES(total_amount),
        profit=VALUES(profit),
        profit_percentage=VALUES(profit_percentage),
        units_sold=VALUES(units_sold),
        inventory_close_u=VALUES(inventory_close_u),
        inventory_close_p=VALUES(inventory_close_p),
        monthly_roi=VALUES(monthly_roi),
        sold_average_month=VALUES(sold_average_month),
        profit_average_month=VALUES(profit_average_month),
        actual_inventory=VALUES(actual_inventory),
        average_selling_cost=VALUES(average_selling_cost),
        inventory_average_u=VALUES(inventory_average_u),
        inventory_average_p=VALUES(inventory_average_p),
        inventory_days=VALUES(inventory_days),
        sales_percentage=VALUES(sales_percentage),
        acc_sales_percentage=VALUES(acc_sales_percentage),
        sold_abc=VALUES(sold_abc),
        acc_profit_percentage=VALUES(acc_profit_percentage),
        profit_abc=VALUES(profit_abc),
        top_products=VALUES(top_products),
        month_sale_u_january=VALUES(month_sale_u_january),
        month_sale_p_january=VALUES(month_sale_p_january),
        inventory_close_u_january=VALUES(inventory_close_u_january),
        inventory_close_p_january=VALUES(inventory_close_p_january),
        month_sale_u_february=VALUES(month_sale_u_february),
        month_sale_p_february=VALUES(month_sale_p_february),
        inventory_close_u_february=VALUES(inventory_close_u_february),
        inventory_close_p_february=VALUES(inventory_close_p_february),
        month_sale_u_march=VALUES(month_sale_u_march),
        month_sale_p_march=VALUES(month_sale_p_march),
        inventory_close_u_march=VALUES(inventory_close_u_march),
        inventory_close_p_march=VALUES(inventory_close_p_march),
        month_sale_u_april=VALUES(month_sale_u_april),
        month_sale_p_april=VALUES(month_sale_p_april),
        inventory_close_u_april=VALUES(inventory_close_u_april),
        inventory_close_p_april=VALUES(inventory_close_p_april),
        month_sale_u_may=VALUES(month_sale_u_may),
        month_sale_p_may=VALUES(month_sale_p_may),
        inventory_close_u_may=VALUES(inventory_close_u_may),
        inventory_close_p_may=VALUES(inventory_close_p_may),
        month_sale_u_june=VALUES(month_sale_u_june),
        month_sale_p_june=VALUES(month_sale_p_june),
        inventory_close_u_june=VALUES(inventory_close_u_june),
        inventory_close_p_june=VALUES(inventory_close_p_june),
        month_sale_u_july=VALUES(month_sale_u_july),
        month_sale_p_july=VALUES(month_sale_p_july),
        inventory_close_u_july=VALUES(inventory_close_u_july),
        inventory_close_p_july=VALUES(inventory_close_p_july),
        month_sale_u_august=VALUES(month_sale_u_august),
        month_sale_p_august=VALUES(month_sale_p_august),
        inventory_close_u_august=VALUES(inventory_close_u_august),
        inventory_close_p_august=VALUES(inventory_close_p_august),
        month_sale_u_september=VALUES(month_sale_u_september),
        month_sale_p_september=VALUES(month_sale_p_september),
        inventory_close_u_september=VALUES(inventory_close_u_september),
        inventory_close_p_september=VALUES(inventory_close_p_september),
        month_sale_u_october=VALUES(month_sale_u_october),
        month_sale_p_october=VALUES(month_sale_p_october),
        inventory_close_u_october=VALUES(inventory_close_u_october),
        inventory_close_p_october=VALUES(inventory_close_p_october),
        month_sale_u_november=VALUES(month_sale_u_november),
        month_sale_p_november=VALUES(month_sale_p_november),
        inventory_close_u_november=VALUES(inventory_close_u_november),
        inventory_close_p_november=VALUES(inventory_close_p_november),
        month_sale_u_december=VALUES(month_sale_u_december),
        month_sale_p_december=VALUES(month_sale_p_december),
        inventory_close_u_december=VALUES(inventory_close_u_december),
        inventory_close_p_december=VALUES(inventory_close_p_december),
        last_update=VALUES(last_update)
    ;
    """
    )

