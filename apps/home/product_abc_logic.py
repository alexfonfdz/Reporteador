from core.settings import ENV_PSQL_NAME, ENV_PSQL_USER, ENV_PSQL_PASSWORD, ENV_PSQL_HOST, ENV_PSQL_PORT, ENV_PSQL_DB_SCHEMA, ENV_MYSQL_HOST, ENV_MYSQL_PORT, ENV_MYSQL_NAME, ENV_MYSQL_USER, ENV_MYSQL_PASSWORD
import psycopg2 as p
import mysql.connector as m
import datetime

async def upsert_families():
    try:
        conn = p.connect(dbname= ENV_PSQL_NAME, user=ENV_PSQL_USER, host=ENV_PSQL_HOST, password=ENV_PSQL_PASSWORD, port=ENV_PSQL_PORT)
        cursor = conn.cursor()
        cursor.execute(f"SELECT nombre, id FROM {ENV_PSQL_DB_SCHEMA}.admintotal_linea")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD, database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)
        cursor = conn.cursor()
        cursor.execute("SELECT id, name FROM family")
        rows2 = cursor.fetchall()
        for row in rows:
            if row[0] not in [r[1] for r in rows2]:
                cursor.execute(f"INSERT INTO family (name, id_admin) VALUES (%s, %s)", (row[0], row[1],))
        conn.commit()
        cursor.close()
        conn.close()

        print("Familias insertadas correctamente")
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        if conn:
            conn.close()
        if cursor:
            cursor.close()

async def upsert_subfamilies():
    try:
        conn = p.connect(dbname= ENV_PSQL_NAME, user=ENV_PSQL_USER, host=ENV_PSQL_HOST, password=ENV_PSQL_PASSWORD, port=ENV_PSQL_PORT)
        cursor = conn.cursor()
        cursor.execute(f"SELECT nombre, id FROM {ENV_PSQL_DB_SCHEMA}.admintotal_sublinea")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD, database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)
        cursor = conn.cursor()
        cursor.execute("SELECT id, name FROM subfamily")
        rows2 = cursor.fetchall()
        for row in rows:
            if row[0] not in [r[1] for r in rows2]:
                cursor.execute(f"INSERT INTO subfamily (name, id_admin) VALUES (%s, %s)", (row[0], row[1],))
        conn.commit()
        cursor.close()
        conn.close()

        print("Subfamilias insertadas correctamente")
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        if conn:
            conn.close()
        if cursor:
            cursor.close()

async def upsert_products():
    try:
        conn = p.connect(dbname= ENV_PSQL_NAME, user=ENV_PSQL_USER, host=ENV_PSQL_HOST, password=ENV_PSQL_PASSWORD, port=ENV_PSQL_PORT)
        cursor = conn.cursor()
        cursor.execute(f"""
                       SELECT p.descripcion, p.codigo, p.id, sl.nombre, l.nombre, p.activo, p.sublinea_id, p.linea_id FROM {ENV_PSQL_DB_SCHEMA}.admintotal_producto as p 
                       INNER JOIN {ENV_PSQL_DB_SCHEMA}.admintotal_sublinea as sl ON p.sublinea_id = sl.id 
                       INNER JOIN {ENV_PSQL_DB_SCHEMA}.admintotal_linea as l ON p.linea_id = l.id""")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD, database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)
        cursor = conn.cursor()
        cursor.execute("SELECT id, code FROM product")
        rows2 = cursor.fetchall()        
        for row in rows:            
            if row[1] not in [r[1] for r in rows2]:                                
                cursor.execute(f"INSERT INTO product (description, code, id_admin, family_id, subfamily_id) VALUES (%s, %s, %s, (SELECT id FROM family WHERE id_admin = %s), (SELECT id FROM subfamily WHERE id_admin = %s))", (row[0], row[1], row[2], row[7], row[6]))
            elif row[5] == True:
                print(f"UPDATE product SET description = %s, family_id = (SELECT id FROM family WHERE id_admin = %s), subfamily_id = (SELECT id FROM subfamily WHERE id_admin = %s) WHERE code = %s", (row[0], row[7], row[6], row[1]))
                cursor.execute(f"UPDATE product SET description = %s, family_id = (SELECT id FROM family WHERE id_admin = %s), subfamily_id = (SELECT id FROM subfamily WHERE id_admin = %s) WHERE code = %s", (row[0], row[7], row[6], row[1]))
        conn.commit()
        cursor.close()
        conn.close()

        print("Productos insertados correctamente")
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        if conn:
            conn.close()
        if cursor:
            cursor.close()

async def upsert_catalogs(description):
    try:
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD, database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)
        cursor = conn.cursor()
        if description is None or description == '' or description == ' ' or description == 'N/A' or description == '0' or description == 0:
            return False
        else:
            cursor.execute("SELECT id FROM catalog WHERE description = %s", (description,))
            row = cursor.fetchone()
            if row is None:
                cursor.execute("INSERT INTO catalog (description) VALUES (%s)", (description,))
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        if conn:
            conn.close()
        if cursor:
            cursor.close()

async def upsert_product_catalogs(product_code, catalog_description, year, subfamily):
    try:
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD, database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM product WHERE code = %s", (product_code,))
        product_id = cursor.fetchone()
        if product_id is None:
            return False
        product_id = product_id[0]
        cursor.execute("SELECT id FROM catalog WHERE description = %s", (catalog_description,))
        catalog_id = cursor.fetchone()
        if catalog_id is None:
            return False
        catalog_id = catalog_id[0]
        cursor.execute("SELECT id, product_id, catalog_id, year FROM product_catalog WHERE product_id = %s", (product_id,))
        row = cursor.fetchone()
        if row is not None and row[0] is not None:
            if row[2] != catalog_id or row[3] == year:
                cursor.execute("UPDATE product_catalog SET catalog_id = %s WHERE product_id = %s AND year = %s", (catalog_id, product_id, year))
            else:
                cursor.execute("INSERT INTO product_catalog (product_id, catalog_id, year) VALUES (%s, %s, %s)", (product_id, catalog_id, year))
        elif row[1] == product_id and row[2] == catalog_id and row[3] == year:
            pass
        else:
            cursor.execute("INSERT INTO product_catalog (product_id, catalog_id, year) VALUES (%s, %s, %s)", (product_id, catalog_id, year))
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        if conn:
            conn.close()
        if cursor:
            cursor.close()

async def get_product_catalogs(year):
    try:
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD, database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)
        cursor = conn.cursor()
        # Tomar el ultimo catalogo de cada producto del año seleccionado para atras en caso de que no haya un catalogo para el año seleccionado
        cursor.execute(f"SELECT p.code, c.id, pc.year FROM product_catalog as pc INNER JOIN catalog as c ON pc.catalog_id = c.id INNER JOIN product as p ON pc.product_id = p.id WHERE pc.year <= %s AND pc.year = (SELECT MAX(year) FROM product_catalog WHERE product_id = p.id AND year <= %s)", (year, year))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        # Agrupar los productos por catalogo siendo el catalogo un valor en un diccionario y los productos una tupla
        catalogs = {}
        for row in rows:
            if row[1] not in catalogs:
                catalogs[row[1]] = [row[0]]
            else:
                catalogs[row[1]].append(row[0])
        # Convertir el diccionario en una lista de tuplas
        catalogs_list = [(k, v) for k, v in catalogs.items()]
        # Crear una lista de tuplas con el id del catalogo y los productos
        catalogs_list = [(catalog[0], ', '.join(catalog[1])) for catalog in catalogs_list]

        return catalogs_list
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        if conn:
            conn.close()
        if cursor:
            cursor.close()

async def upsert_product_abc_part1(catalog_list, year):
    try:
        actual_year = datetime.datetime.now().year
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD, database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)
        cursor = conn.cursor()
        cursor.execute("SELECT year FROM product_abc WHERE year = %s", (year,))
        row = cursor.fetchone()
        last_month_update = 0
        if row is not None and row[0] is not None:
            last_month_update = row[0].month
        if row is None and year < actual_year or (year+1 == actual_year and last_month_update < 3):
            for catalog in catalog_list:
                conn_pg = p.connect(dbname= ENV_PSQL_NAME, user=ENV_PSQL_USER, host=ENV_PSQL_HOST, password=ENV_PSQL_PASSWORD, port=ENV_PSQL_PORT)
                cursor_pg = conn_pg.cursor()
                cursor_pg.execute(f"""SELECT
                                        li.id AS FAMILIA,
                                        sl.id AS SUBFAMILIA,
                                        ROUND(SUM(md.IMPORTE),2) AS TOTAL_IMPORTE,
                                        ROUND(SUM(md.IMPORTE - (md.COSTO_VENTA * md.CANTIDAD)),2) AS TOTAL_UTILIDAD,
                                        ROUND(SUM(md.CANTIDAD),2) AS UNIDADES_VENDIDAS
                                    FROM
                                        MARW.ADMINTOTAL_MOVIMIENTODETALLE md
                                        INNER JOIN MARW.ADMINTOTAL_MOVIMIENTO m ON md.MOVIMIENTO_ID = m.POLIZA_PTR_ID
                                        INNER JOIN MARW.ADMINTOTAL_CLIENTE cl ON m.PROVEEDOR_ID = cl.ID
                                        INNER JOIN MARW.UTILS_CONDICION cond ON cl.CONDICION_ID = cond.ID
                                        INNER JOIN MARW.ADMINTOTAL_POLIZA pol ON m.POLIZA_PTR_ID = pol.ID
                                        LEFT OUTER JOIN MARW.ADMINTOTAL_ALMACEN a ON m.ALMACEN_ID = a.ID
                                        INNER JOIN MARW.ADMINTOTAL_PRODUCTO prod ON md.PRODUCTO_ID = prod.ID
                                        INNER JOIN MARW.ADMINTOTAL_LINEA li ON prod.LINEA_ID = li.ID
                                        INNER JOIN MARW.ADMINTOTAL_SUBLINEA sl ON prod.SUBLINEA_ID = sl.ID
                                        LEFT OUTER JOIN MARW.ADMINTOTAL_UM um ON md.UM_ID = um.ID
                                    WHERE
                                        m.TIPO_MOVIMIENTO = 2
                                        AND EXTRACT(YEAR FROM TIMEZONE('America/Mexico_City', pol.FECHA)) = %s
                                        AND m.CANCELADO = false
                                        AND prod.CODIGO IN %s
                                    GROUP BY
                                        li.id,
                                        sl.id,
                                        li.NOMBRE,
                                        sl.NOMBRE,
                                        EXTRACT(YEAR FROM TIMEZONE('America/Mexico_City', pol.FECHA))
                                    ORDER BY
                                        TOTAL_IMPORTE DESC;""", (year, tuple(catalog[1].split(', '))))
                rows = cursor_pg.fetchall()
                cursor_pg.close()
                conn_pg.close()
                for row in rows:
                    cursor.execute(f"SELECT id FROM family WHERE id_admin = %s", (row[0],))
                    family_id = cursor.fetchone()
                    if family_id is None:
                        return False
                    family_id = family_id[0]
                    cursor.execute(f"SELECT id FROM subfamily WHERE id_admin = %s", (row[1],))
                    subfamily_id = cursor.fetchone()
                    if subfamily_id is None:
                        return False
                    subfamily_id = subfamily_id[0]
                    cursor.execute(f"""INSERT INTO product_abc (catalog_id, family_id, 
                                        subfamily_id, total_amount, profit, units_sold, year) 
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", (catalog[0], family_id, subfamily_id, row[2], row[3], row[4], year))
        elif year == actual_year:
            for catalog in catalog_list:
                conn_pg = p.connect(dbname= ENV_PSQL_NAME, user=ENV_PSQL_USER, host=ENV_PSQL_HOST, password=ENV_PSQL_PASSWORD, port=ENV_PSQL_PORT)
                cursor_pg = conn_pg.cursor()
                cursor_pg.execute(f"""SELECT
                                        li.NOMBRE AS FAMILIA,
                                        sl.NOMBRE AS SUBFAMILIA,
                                        ROUND(SUM(md.IMPORTE),2) AS TOTAL_IMPORTE,
                                        ROUND(SUM(md.IMPORTE - (md.COSTO_VENTA * md.CANTIDAD)),2) AS TOTAL_UTILIDAD,
                                        ROUND(SUM(md.CANTIDAD),2) AS UNIDADES_VENDIDAS
                                    FROM
                                        MARW.ADMINTOTAL_MOVIMIENTODETALLE md
                                        INNER JOIN MARW.ADMINTOTAL_MOVIMIENTO m ON md.MOVIMIENTO_ID = m.POLIZA_PTR_ID
                                        INNER JOIN MARW.ADMINTOTAL_CLIENTE cl ON m.PROVEEDOR_ID = cl.ID
                                        INNER JOIN MARW.UTILS_CONDICION cond ON cl.CONDICION_ID = cond.ID
                                        INNER JOIN MARW.ADMINTOTAL_POLIZA pol ON m.POLIZA_PTR_ID = pol.ID
                                        LEFT OUTER JOIN MARW.ADMINTOTAL_ALMACEN a ON m.ALMACEN_ID = a.ID
                                        INNER JOIN MARW.ADMINTOTAL_PRODUCTO prod ON md.PRODUCTO_ID = prod.ID
                                        INNER JOIN MARW.ADMINTOTAL_LINEA li ON prod.LINEA_ID = li.ID
                                        INNER JOIN MARW.ADMINTOTAL_SUBLINEA sl ON prod.SUBLINEA_ID = sl.ID
                                        LEFT OUTER JOIN MARW.ADMINTOTAL_UM um ON md.UM_ID = um.ID
                                    WHERE
                                        m.TIPO_MOVIMIENTO = 2
                                        AND EXTRACT(YEAR FROM TIMEZONE('America/Mexico_City', pol.FECHA)) = %s
                                        AND m.CANCELADO = false
                                        AND prod.CODIGO IN %s
                                    GROUP BY
                                        li.id,
                                        sl.id,
                                        li.NOMBRE,
                                        sl.NOMBRE,
                                        EXTRACT(YEAR FROM TIMEZONE('America/Mexico_City', pol.FECHA))
                                    ORDER BY
                                        TOTAL_IMPORTE DESC;""", (year, tuple(catalog[1].split(', '))))
                rows = cursor_pg.fetchall()
                cursor_pg.close()
                conn_pg.close()

                for row in rows:
                    cursor.execute(f"SELECT id FROM family WHERE name = %s", (row[0],))
                    family_id = cursor.fetchone()
                    if family_id is None:
                        return False
                    family_id = family_id[0]
                    cursor.execute(f"SELECT id FROM subfamily WHERE name = %s", (row[1],))
                    subfamily_id = cursor.fetchone()
                    if subfamily_id is None:
                        return False
                    subfamily_id = subfamily_id[0]
                    cursor.execute(f"SELECT id FROM product_abc WHERE catalog_id = %s AND family_id = %s AND subfamily_id = %s AND year = %s", (catalog[0], family_id, subfamily_id, year))
                    row2 = cursor.fetchone()
                    if row2 is None:
                        cursor.execute(f"""INSERT INTO product_abc (catalog_id, family_id, 
                                        subfamily_id, total_amount, profit, units_sold, year) 
                                        VALUES (%s, %s, %s, %s, %s, %s, %s)""", (catalog[0], family_id, subfamily_id, row[2], row[3], row[4], year))
                    else:
                        cursor.execute(f"""UPDATE product_abc SET total_amount = %s, profit = %s, units_sold = %s 
                                        WHERE catalog_id = %s AND family_id = %s AND subfamily_id = %s AND year = %s""", (row[2], row[3], row[4], catalog[0], family_id, subfamily_id, year))
        else:
            pass                 
            conn.commit()
            cursor.close()
            conn.close()
            return True
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        if conn:
            conn.close()
        if cursor:
            cursor.close()

async def upsert_product_abc_part2(year):
    try:
        actual_year = datetime.datetime.now().year
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD, database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)
        cursor = conn.cursor()
        last_month_update = 0
        cursor.execute("SELECT MAX(last_update) FROM product_abc WHERE year = %s", (year,))
        row = cursor.fetchone()
        if row is not None and row[0] is not None:
            last_month_update = row[0].month
        if year < actual_year or (year+1 == actual_year and last_month_update < 3):
            cursor.execute("SELECT id, catalog_id FROM product_abc WHERE year = %s AND (inventory_close_u IS NULL OR inventory_close_p IS NULL)", (year,))
            rows = cursor.fetchall()

            for row in rows:
                row_catalog_id = row[1]
                cursor.execute(f"SELECT product_id FROM product_catalog WHERE catalog_id = %s AND year <= %s AND year = (SELECT MAX(year) FROM product_catalog WHERE catalog_id = %s AND year <= %s)", (row_catalog_id, year, row_catalog_id, year))
                rows2 = cursor.fetchall()
                product_tuple = tuple([r[0] for r in rows2])
                if product_tuple:
                    conn_pg = p.connect(dbname= ENV_PSQL_NAME, user=ENV_PSQL_USER, host=ENV_PSQL_HOST, password=ENV_PSQL_PASSWORD, port=ENV_PSQL_PORT)
                    cursor_pg = conn_pg.cursor()
                    cursor_pg.execute(f"""SELECT 
                                            ROUND(SUM(sub.existencias),2) AS EXISTENCIAS,
                                            ROUND(SUM(sub.existencias_pesos),2) AS EXISTENCIAS_P
                                        FROM (SELECT
                                                ROUND(SUM(CASE
                                                WHEN m_inv.es_entrada = true then md_inv.cantidad
                                                WHEN m_inv.es_salida = true then -md_inv.cantidad
                                                END),5)AS existencias,
                                                ROUND(SUM(CASE
                                                WHEN m_inv.es_entrada = true then md_inv.cantidad*md_inv.costo_venta
                                                WHEN m_inv.es_salida = true then -md_inv.cantidad*md_inv.costo_venta
                                                END),5)AS existencias_pesos
                                                FROM marw.admintotal_movimientodetalle md_inv
                                                INNER JOIN marw.admintotal_movimiento m_inv ON m_inv.poliza_ptr_id = md_inv.movimiento_id
                                                INNER JOIN marw.admintotal_poliza p_inv_lookup ON m_inv.poliza_ptr_id = p_inv_lookup.id
                                                LEFT OUTER JOIN MARW.ADMINTOTAL_ALMACEN a ON m_inv.ALMACEN_ID = a.ID
                                                INNER JOIN marw.admintotal_producto prod_inv ON md_inv.producto_id = prod_inv.id
                                                WHERE m_inv.pendiente = true
                                                AND (
                                                    EXTRACT(YEAR FROM TIMEZONE('America/Mexico_City', p_inv_lookup.FECHA)) < %s OR
                                                    (EXTRACT(YEAR FROM TIMEZONE('America/Mexico_City', p_inv_lookup.FECHA)) = %s AND EXTRACT(MONTH FROM TIMEZONE('America/Mexico_City', p_inv_lookup.FECHA)) <= 12)
                                                )
                                                AND prod_inv.CODIGO IN %s
                                                GROUP BY
                                                m_inv.almacen_id,
                                                md_inv.producto_id
                                        )  sub """, (year, year, product_tuple))

                    rows3 = cursor_pg.fetchall()
                    cursor_pg.close()
                    conn_pg.close()
                    for row3 in rows3:
                        cursor.execute(f"UPDATE product_abc SET inventory_close_u = %s, inventory_close_p = %s WHERE id = %s", (row3[0], row3[1], row[0]))
            conn.commit()
            cursor.close()
            conn.close()
            return True
        elif year == actual_year:
            cursor.execute("SELECT id, catalog_id FROM product_abc WHERE year = %s", (year,))
            rows = cursor.fetchall()

            for row in rows:
                row_catalog_id = row[1]
                cursor.execute(f"SELECT product_id FROM product_catalog WHERE catalog_id = %s AND year <= %s AND year = (SELECT MAX(year) FROM product_catalog WHERE catalog_id = %s AND year <= %s)", (row_catalog_id, year, row_catalog_id, year))
                rows2 = cursor.fetchall()
                product_tuple = tuple([r[0] for r in rows2])
                if product_tuple:
                    conn_pg = p.connect(dbname= ENV_PSQL_NAME, user=ENV_PSQL_USER, host=ENV_PSQL_HOST, password=ENV_PSQL_PASSWORD, port=ENV_PSQL_PORT)
                    cursor_pg = conn_pg.cursor()
                    cursor_pg.execute(f"""SELECT
                                            ROUND(SUM(sub.existencia), 2) AS INVENTARIO_CIERRE_U,
                                            ROUND(SUM(sub.existencia*sub.costo_venta), 2) AS INVENTARIO_CIERRE_P
                                        FROM (
                                            SELECT
                                            md_inv.pxa_id,
                                            md_inv.existencia,
                                            md_inv.costo_venta,
                                            ROW_NUMBER() OVER (PARTITION BY md_inv.pxa_id ORDER BY p_inv_lookup.fecha DESC, p_inv_lookup.id DESC, md_inv.id DESC) AS rn
                                            FROM marw.admintotal_movimientodetalle md_inv
                                            INNER JOIN marw.admintotal_movimiento m_inv ON m_inv.poliza_ptr_id = md_inv.movimiento_id
                                            INNER JOIN marw.admintotal_poliza p_inv_lookup ON m_inv.poliza_ptr_id = p_inv_lookup.id
                                            INNER JOIN marw.admintotal_productoalmacen pxa_inv ON md_inv.pxa_id = pxa_inv.id
                                            INNER JOIN marw.admintotal_producto prod_inv ON pxa_inv.producto_id = prod_inv.id
                                            WHERE md_inv.pxa_id IS NOT NULL
                                            AND m_inv.tipo_movimiento = 2
                                            AND m_inv.cancelado = false
                                            AND EXTRACT(YEAR FROM TIMEZONE('America/Mexico_City', p_inv_lookup.FECHA)) <= %s
                                            AND prod_inv.CODIGO IN %s
                                        ) sub
                                        WHERE
                                        sub.rn = 1;""", (year, product_tuple))
                    rows3 = cursor_pg.fetchall()
                    cursor_pg.close()
                    conn_pg.close()
                    for row3 in rows3:
                        cursor.execute(f"SELECT id FROM product_abc WHERE catalog_id = %s AND year = %s", (row_catalog_id, year))
                        row2 = cursor.fetchone()
                        if row2 is None:
                            cursor.execute(f"INSERT INTO product_abc (catalog_id, inventory_close_u, inventory_close_p) VALUES (%s, %s, %s)", (row_catalog_id, row3[0], row3[1]))
                        else:
                            cursor.execute(f"UPDATE product_abc SET inventory_close_u = %s, inventory_close_p = %s WHERE id = %s", (row3[0], row3[1], row2[0]))
            conn.commit()
            cursor.close()
            conn.close()
            return True
        else:
            pass
            conn.commit()
            cursor.close()
            conn.close()
            return True
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        if conn:
            conn.close()
        if cursor:
            cursor.close()

async def upsert_product_abc_part3(year):
    try:
        actual_year = datetime.datetime.now().year
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD, database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)
        cursor = conn.cursor()
        columns_months = ['inventory_close_u_january', 'inventory_close_p_january', 
                          'inventory_close_u_february', 'inventory_close_p_february', 
                          'inventory_close_u_march', 'inventory_close_p_march', 
                          'inventory_close_u_april', 'inventory_close_p_april', 
                          'inventory_close_u_may', 'inventory_close_p_may', 
                          'inventory_close_u_june', 'inventory_close_p_june', 
                          'inventory_close_u_july', 'inventory_close_p_july', 
                          'inventory_close_u_august', 'inventory_close_p_august', 
                          'inventory_close_u_september', 'inventory_close_p_september', 
                          'inventory_close_u_october', 'inventory_close_p_october', 
                          'inventory_close_u_november', 'inventory_close_p_november', 
                          'inventory_close_u_december', 'inventory_close_p_december']
        cursor.execute("SELECT MAX(last_update) FROM product_abc WHERE year = %s", (year,))
        row = cursor.fetchone()
        last_month_update = 0
        if row is not None and row[0] is not None:
            last_month_update = row[0].month

        if year < actual_year or (year+1 == actual_year and last_month_update < 3):
            query = f"SELECT id, catalog_id FROM product_abc WHERE year = %s AND ({' OR '.join([f'{col} IS NULL' for col in columns_months])})"
            cursor.execute(query, (year,))
            rows = cursor.fetchall()

            for row in rows:
                row_catalog_id = row[1]
                cursor.execute(f"SELECT product_id FROM product_catalog WHERE catalog_id = %s AND year <= %s AND year = (SELECT MAX(year) FROM product_catalog WHERE catalog_id = %s AND year <= %s)", (row_catalog_id, year, row_catalog_id, year))
                rows2 = cursor.fetchall()
                product_tuple = tuple([r[0] for r in rows2])
                if product_tuple:
                    conn_pg = p.connect(dbname= ENV_PSQL_NAME, user=ENV_PSQL_USER, host=ENV_PSQL_HOST, password=ENV_PSQL_PASSWORD, port=ENV_PSQL_PORT)
                    cursor_pg = conn_pg.cursor()
                    for month in range(1, 13):
                        cursor_pg.execute(f"""SELECT
                                                ROUND(SUM(sub.existencias),2) AS EXISTENCIAS,
                                                ROUND(SUM(sub.existencias_pesos),2) AS EXISTENCIAS_P
                                            FROM (SELECT
                                                    ROUND(SUM(CASE
                                                    WHEN m_inv.es_entrada = true then md_inv.cantidad
                                                    WHEN m_inv.es_salida = true then -md_inv.cantidad
                                                    END),5)AS existencias,
                                                    ROUND(SUM(CASE
                                                    WHEN m_inv.es_entrada = true then md_inv.cantidad*md_inv.costo_venta
                                                    WHEN m_inv.es_salida = true then -md_inv.cantidad*md_inv.costo_venta
                                                    END),5)AS existencias_pesos
                                                    FROM marw.admintotal_movimientodetalle md_inv
                                                    INNER JOIN marw.admintotal_movimiento m_inv ON m_inv.poliza_ptr_id = md_inv.movimiento_id
                                                    INNER JOIN marw.admintotal_poliza p_inv_lookup ON m_inv.poliza_ptr_id = p_inv_lookup.id
                                                    LEFT OUTER JOIN MARW.ADMINTOTAL_ALMACEN a ON m_inv.ALMACEN_ID = a.ID
                                                    INNER JOIN marw.admintotal_producto prod_inv ON md_inv.producto_id = prod_inv.id
                                                    WHERE m_inv.pendiente = true
                                                    AND (
                                                        EXTRACT(YEAR FROM TIMEZONE('America/Mexico_City', p_inv_lookup.FECHA)) < %s OR
                                                        (EXTRACT(YEAR FROM TIMEZONE('America/Mexico_City', p_inv_lookup.FECHA)) = %s AND EXTRACT(MONTH FROM TIMEZONE('America/Mexico_City', p_inv_lookup.FECHA)) <= %s)
                                                    )
                                                    AND prod_inv.CODIGO IN %s
                                                    GROUP BY
                                                    m_inv.almacen_id,
                                                    md_inv.producto_id
                                            )  sub """, (year, year, month, product_tuple))
                        rows3 = cursor_pg.fetchall()
                        for row3 in rows3:
                            cursor.execute(f"UPDATE product_abc SET {columns_months[(month-1)*2]} = %s, {columns_months[(month-1)*2+1]} = %s WHERE id = %s", (row3[0], row3[1], row[0]))
                    cursor_pg.close()
                    conn_pg.close()
            conn.commit()
            cursor.close()
            conn.close()
            return True
        elif year == actual_year:
            cursor.execute("SELECT id, catalog_id FROM product_abc WHERE year = %s", (year,))
            rows = cursor.fetchall()

            for row in rows:
                row_catalog_id = row[1]
                cursor.execute(f"SELECT product_id FROM product_catalog WHERE catalog_id = %s AND year <= %s AND year = (SELECT MAX(year) FROM product_catalog WHERE catalog_id = %s AND year <= %s)", (row_catalog_id, year, row_catalog_id, year))
                rows2 = cursor.fetchall()
                product_tuple = tuple([r[0] for r in rows2])
                if product_tuple:
                    conn_pg = p.connect(dbname= ENV_PSQL_NAME, user=ENV_PSQL_USER, host=ENV_PSQL_HOST, password=ENV_PSQL_PASSWORD, port=ENV_PSQL_PORT)
                    cursor_pg = conn_pg.cursor()
                    for month in range(1, 13):
                        cursor_pg.execute(f"""SELECT
                                                ROUND(SUM(sub.existencia), 2) AS INVENTARIO_CIERRE_U,
                                                ROUND(SUM(sub.existencia*sub.costo_venta), 2) AS INVENTARIO_CIERRE_P
                                            FROM (
                                                SELECT
                                                md_inv.pxa_id,
                                                md_inv.existencia,
                                                md_inv.costo_venta,
                                                ROW_NUMBER() OVER (PARTITION BY md_inv.pxa_id ORDER BY p_inv_lookup.fecha DESC, p_inv_lookup.id DESC, md_inv.id DESC) AS rn
                                                FROM marw.admintotal_movimientodetalle md_inv
                                                INNER JOIN marw.admintotal_movimiento m_inv ON m_inv.poliza_ptr_id = md_inv.movimiento_id
                                                INNER JOIN marw.admintotal_poliza p_inv_lookup ON m_inv.poliza_ptr_id = p_inv_lookup.id
                                                INNER JOIN marw.admintotal_productoalmacen pxa_inv ON md_inv.pxa_id = pxa_inv.id
                                                INNER JOIN marw.admintotal_producto prod_inv ON pxa_inv.producto_id = prod_inv.id
                                                WHERE md_inv.pxa_id IS NOT NULL
                                                AND m_inv.tipo_movimiento = 2
                                                AND m_inv.cancelado = false
                                                AND EXTRACT(YEAR FROM TIMEZONE('America/Mexico_City', p_inv_lookup.FECHA)) <= %s
                                                AND EXTRACT(MONTH FROM TIMEZONE('America/Mexico_City', p_inv_lookup.FECHA)) <= %s
                                                AND prod_inv.CODIGO IN %s
                                            ) sub WHERE sub.rn = 1;""", (year, month, product_tuple))
                        rows3 = cursor_pg.fetchall()
                        for row3 in rows3:
                            cursor.execute(f"UPDATE product_abc SET {columns_months[(month-1)*2]} = %s, {columns_months[(month-1)*2+1]} = %s WHERE id = %s", (row3[0], row3[1], row[0]))
                    cursor_pg.close()
                    conn_pg.close()
            conn.commit()
            cursor.close()
            conn.close()
            return True
        else:
            pass
            conn.commit()
            cursor.close()
            conn.close()
            return True
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        if conn:
            conn.close()
        if cursor:
            cursor.close()

async def upsert_product_abc_part4(year):
    # faltantes: profit_percentage, monthly_roi, sold_average_month, profit_average_month, actual_inventory, average_selling_cost, inventory_average_u, inventory_average_p, inventory_days, sales_percentage, acc_sales_percentage, sold_abc, profit_percentage, acc_profit_percentage, profit_abc, top_products
    try:
        actual_year = datetime.datetime.now().year
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD, database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(last_update) FROM product_abc WHERE year = %s", (year,))
        row = cursor.fetchone()
        last_month_update = 0
        if row is not None and row[0] is not None:
            last_month_update = row[0].month

        if year < actual_year or (year+1 == actual_year and last_month_update < 3):
            cursor.execute("SELECT id FROM product_abc WHERE year = %s AND (monthly_roi IS NULL OR sold_average_month IS NULL OR profit_average_month IS NULL OR actual_inventory IS NULL OR average_selling_cost IS NULL OR inventory_average_u IS NULL OR inventory_average_p IS NULL OR inventory_days IS NULL OR sales_percentage IS NULL OR acc_sales_percentage IS NULL)", (year,))
            rows = cursor.fetchall()

            column_months = ['inventory_close_u_january', 'inventory_close_p_january', 
                          'inventory_close_u_february', 'inventory_close_p_february', 
                          'inventory_close_u_march', 'inventory_close_p_march', 
                          'inventory_close_u_april', 'inventory_close_p_april', 
                          'inventory_close_u_may', 'inventory_close_p_may', 
                          'inventory_close_u_june', 'inventory_close_p_june', 
                          'inventory_close_u_july', 'inventory_close_p_july', 
                          'inventory_close_u_august', 'inventory_close_p_august', 
                          'inventory_close_u_september', 'inventory_close_p_september', 
                          'inventory_close_u_october', 'inventory_close_p_october', 
                          'inventory_close_u_november', 'inventory_close_p_november', 
                          'inventory_close_u_december', 'inventory_close_p_december']
    
            for row in rows:
                cursor.execute(f"SELECT {', '.join(column_months)} FROM product_abc WHERE id = %s", (row[0],))
                row2 = cursor.fetchone()
                if row2 is not None:
                    # Sumar todos los inventory_close_u y inventory_close_p de los meses
                    total_inventory_close_u = sum(row2[idx] if row2[idx] is not None else 0 for idx, col in enumerate(column_months) if 'inventory_close_u' in col)
                    total_inventory_close_p = sum(row2[idx] if row2[idx] is not None else 0 for idx, col in enumerate(column_months) if 'inventory_close_p' in col)
                    cursor.execute(f"UPDATE product_abc SET inventory_average_u = %s, inventory_average_p = %s WHERE id = %s", (total_inventory_close_u, total_inventory_close_p,  row[0]))
                    conn.commit()
                    cursor.execute(f"SELECT total_amount, profit, inventory_close_u, inventory_close_u, inventory_average_u, inventory_average_p FROM product_abc WHERE id = %s", (row[0],))
                    row3 = cursor.fetchone()

                    if row3 is not None:
                        profit_average_month = row3[1] / 12
                        monthly_roi = (profit_average_month / row3[5] ) * 100 if row3[5] > 0 else 0
                        sold_average_month = row3[0] / 12
                        actual_inventory = row3[3]
                        average_selling_cost = (row3[0] - row3[1]) / 12
                        inventory_days = (actual_inventory / sold_average_month) * 30 if sold_average_month > 0 else 0

                        cursor.execute(f"UPDATE product_abc SET monthly_roi = %s, sold_average_month = %s, profit_average_month = %s, actual_inventory = %s, average_selling_cost = %s, inventory_days = %s WHERE id = %s", (monthly_roi, sold_average_month, profit_average_month, actual_inventory, average_selling_cost, inventory_days, row[0]))
                        conn.commit()

                    else:
                        return False
                else:
                    return False
            conn.commit()
            cursor.close()
            conn.close()
            return True
        elif year == actual_year:
            cursor.execute("SELECT id FROM product_abc WHERE year = %s", (year,))
            rows = cursor.fetchall()

            column_months = ['inventory_close_u_january', 'inventory_close_p_january', 
                          'inventory_close_u_february', 'inventory_close_p_february', 
                          'inventory_close_u_march', 'inventory_close_p_march', 
                          'inventory_close_u_april', 'inventory_close_p_april', 
                          'inventory_close_u_may', 'inventory_close_p_may', 
                          'inventory_close_u_june', 'inventory_close_p_june', 
                          'inventory_close_u_july', 'inventory_close_p_july', 
                          'inventory_close_u_august', 'inventory_close_p_august', 
                          'inventory_close_u_september', 'inventory_close_p_september', 
                          'inventory_close_u_october', 'inventory_close_p_october', 
                          'inventory_close_u_november', 'inventory_close_p_november', 
                          'inventory_close_u_december', 'inventory_close_p_december']

            for row in rows:
                cursor.execute(f"SELECT {', '.join(column_months)} FROM product_abc WHERE id = %s", (row[0],))
                row2 = cursor.fetchone()
                if row2 is not None:
                    # Sumar todos los inventory_close_u y inventory_close_p de los meses
                    total_inventory_close_u = sum(row2[idx] if row2[idx] is not None else 0 for idx, col in enumerate(column_months) if 'inventory_close_u' in col)
                    total_inventory_close_p = sum(row2[idx] if row2[idx] is not None else 0 for idx, col in enumerate(column_months) if 'inventory_close_p' in col)
                    cursor.execute(f"UPDATE product_abc SET inventory_average_u = %s, inventory_average_p = %s WHERE id = %s", (total_inventory_close_u, total_inventory_close_p,  row[0]))
                    conn.commit()
                    cursor.execute(f"SELECT total_amount, profit, inventory_average_u, inventory_average_p FROM product_abc WHERE id = %s", (row[0],))
                    row3 = cursor.fetchone()

                    if row3 is not None:
                        profit_average_month = row3[1] / 12
                        monthly_roi = (profit_average_month / row3[3] ) * 100 if row3[3] > 0 else 0
                        sold_average_month = row3[0] / 12
                        actual_inventory = row3[2]
                        average_selling_cost = (row3[0] - row3[1]) / 12
                        inventory_days = (actual_inventory / sold_average_month) * 30 if sold_average_month > 0 else 0

                        cursor.execute(f"UPDATE product_abc SET monthly_roi = %s, sold_average_month = %s, profit_average_month = %s, actual_inventory = %s, average_selling_cost = %s, inventory_days = %s WHERE id = %s", (monthly_roi, sold_average_month, profit_average_month, actual_inventory, average_selling_cost, inventory_days, row[0]))
                        conn.commit()

                    else:
                        return False
                else:
                    return False
            conn.commit()
            cursor.close()
            conn.close()
            return True
        else:
            pass
            conn.commit()
            cursor.close()
            conn.close()
            return True
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        if conn:
            conn.close()
        if cursor:
            cursor.close()

async def upsert_product_abc_part5(year):
    try:
        actual_year = datetime.datetime.now().year
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD, database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(last_update) FROM product_abc WHERE year = %s", (year,))
        row = cursor.fetchone()
        last_month_update = 0
        if row is not None and row[0] is not None:
            last_month_update = row[0].month

        if year < actual_year or (year+1 == actual_year and last_month_update < 3):
            cursor.execute("SELECT id FROM product_abc WHERE year = %s AND (sales_percentage IS NULL OR acc_sales_percentage IS NULL) ORDER BY total_amount DESC", (year,))
            rows = cursor.fetchall()
            cursor.execute("SELECT SUM(total_amount) FROM product_abc WHERE year = %s", (year,))
            total_general_amount = cursor.fetchone()
            if total_general_amount is not None:
                total_general_amount = total_general_amount[0]
            else:
                total_general_amount = 0
            
            acc_sales_percentage = 0
            for row in rows:
                cursor.execute(f"SELECT total_amount FROM product_abc WHERE id = %s", (row[0],))
                row2 = cursor.fetchone()
                if row2 is not None:
                    sales_percentage = (row2[0] / total_general_amount) * 100 if total_general_amount > 0 else 0
                    acc_sales_percentage += (row2[0] / total_general_amount) * 100 if total_general_amount > 0 else 0
                    cursor.execute(f"UPDATE product_abc SET sales_percentage = %s, acc_sales_percentage = %s WHERE id = %s", (sales_percentage, acc_sales_percentage, row[0]))
                    conn.commit()
                else:
                    return False
            conn.commit()
            cursor.close()
            conn.close()
            return True
        elif year == actual_year:
            cursor.execute("SELECT id FROM product_abc WHERE year = %s", (year,))
            rows = cursor.fetchall()
            cursor.execute("SELECT SUM(total_amount) FROM product_abc WHERE year = %s", (year,))
            total_general_amount = cursor.fetchone()
            if total_general_amount is not None:
                total_general_amount = total_general_amount[0]
            else:
                total_general_amount = 0

            acc_sales_percentage = 0
            for row in rows:
                cursor.execute(f"SELECT total_amount FROM product_abc WHERE id = %s", (row[0],))
                row2 = cursor.fetchone()
                if row2 is not None:
                    sales_percentage = (row2[0] / total_general_amount) * 100 if total_general_amount > 0 else 0
                    acc_sales_percentage += (row2[0] / total_general_amount) * 100 if total_general_amount > 0 else 0
                    sold_abc = ''
                    if acc_sales_percentage <= 80:
                        sold_abc = 'A'
                    elif acc_sales_percentage <= 95:
                        sold_abc = 'B'
                    else:
                        sold_abc = 'C'
                    cursor.execute(f"UPDATE product_abc SET sales_percentage = %s, acc_sales_percentage = %s, sold_abc = %s WHERE id = %s", (sales_percentage, acc_sales_percentage, sold_abc, row[0]))
                    conn.commit()
                else:
                    return False
            conn.commit()
            cursor.close()
            conn.close()
            return True
        else:
            pass
            conn.commit()
            cursor.close()
            conn.close()
            return True
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        if conn:
            conn.close()
        if cursor:
            cursor.close()

async def upsert_product_abc_part6(year):
    try:
        actual_year = datetime.datetime.now().year
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD, database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(last_update) FROM product_abc WHERE year = %s", (year,))
        row = cursor.fetchone()
        last_month_update = 0
        if row is not None and row[0] is not None:
            last_month_update = row[0].month

        if year < actual_year or (year+1 == actual_year and last_month_update < 3):
            cursor.execute("SELECT id FROM product_abc WHERE year = %s AND (profit_percentage IS NULL OR acc_profit_percentage IS NULL) ORDER BY total_amount DESC", (year,))
            rows = cursor.fetchall()
            cursor.execute("SELECT SUM(profit) FROM product_abc WHERE year = %s", (year,))
            total_general_profit = cursor.fetchone()
            if total_general_profit is not None:
                total_general_profit = total_general_profit[0]
            else:
                total_general_profit = 0

            acc_profit_percentage = 0
            for row in rows:
                cursor.execute(f"SELECT profit FROM product_abc WHERE id = %s", (row[0],))
                row2 = cursor.fetchone()
                if row2 is not None:
                    profit_percentage = (row2[0] / total_general_profit) * 100 if total_general_profit > 0 else 0
                    acc_profit_percentage += (row2[0] / total_general_profit) * 100 if total_general_profit > 0 else 0
                    profit_abc = ''
                    if acc_profit_percentage <= 80:
                        profit_abc = 'A'
                    elif acc_profit_percentage <= 95:
                        profit_abc = 'B'
                    else:
                        profit_abc = 'C'
                    cursor.execute(f"UPDATE product_abc SET profit_percentage = %s, acc_profit_percentage = %s, profit_abc = %s WHERE id = %s", (profit_percentage, acc_profit_percentage, profit_abc, row[0]))
                    conn.commit()
                else:
                    return False
            conn.commit()
            cursor.close()
            conn.close()
            return True
        elif year == actual_year:
            cursor.execute("SELECT id FROM product_abc WHERE year = %s", (year,))
            rows = cursor.fetchall()
            cursor.execute("SELECT SUM(profit) FROM product_abc WHERE year = %s", (year,))
            total_general_profit = cursor.fetchone()
            if total_general_profit is not None:
                total_general_profit = total_general_profit[0]
            else:
                total_general_profit = 0

            acc_profit_percentage = 0
            for row in rows:
                cursor.execute(f"SELECT profit FROM product_abc WHERE id = %s", (row[0],))
                row2 = cursor.fetchone()
                if row2 is not None:
                    profit_percentage = (row2[0] / total_general_profit) * 100 if total_general_profit > 0 else 0
                    acc_profit_percentage += (row2[0] / total_general_profit) * 100 if total_general_profit > 0 else 0
                    profit_abc = ''
                    if acc_profit_percentage <= 80:
                        profit_abc = 'A'
                    elif acc_profit_percentage <= 95:
                        profit_abc = 'B'
                    else:
                        profit_abc = 'C'
                    cursor.execute(f"UPDATE product_abc SET profit_percentage = %s, acc_profit_percentage = %s, profit_abc = %s WHERE id = %s", (profit_percentage, acc_profit_percentage, profit_abc, row[0]))
                    conn.commit()
                else:
                    return False
            conn.commit()
            cursor.close()
            conn.close()
            return True
        else:
            pass
            conn.commit()
            cursor.close()
            conn.close()
            return True
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        if conn:
            conn.close()
        if cursor:
            cursor.close()

async def upsert_product_abc_part7(year):
    try:
        actual_year = datetime.datetime.now().year
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD, database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(last_update) FROM product_abc WHERE year = %s", (year,))
        row = cursor.fetchone()
        last_month_update = 0
        if row is not None and row[0] is not None:
            last_month_update = row[0].month

        if year < actual_year or (year+1 == actual_year and last_month_update < 3):
            cursor.execute("SELECT id FROM product_abc WHERE year = %s AND (top_products IS NULL)", (year,))
            rows = cursor.fetchall()

            for row in rows:
                cursor.execute(f"SELECT sold_abc, profit_abc FROM product_abc WHERE id = %s", (row[0],))
                row2 = cursor.fetchone()

                if row2 is not None:
                    sold_abc = row2[0]
                    profit_abc = row2[1]

                    if sold_abc == 'A' and profit_abc == 'A':
                        top_products = 'AA'
                    else:
                        top_products = ''
                    cursor.execute(f"UPDATE product_abc SET top_products = %s WHERE id = %s", (top_products, row[0]))
                    conn.commit()
                else:
                    return False
            conn.commit()
            cursor.close()
            conn.close()
            return True
        elif year == actual_year:
            cursor.execute("SELECT id FROM product_abc WHERE year = %s", (year,))
            rows = cursor.fetchall()

            for row in rows:
                cursor.execute(f"SELECT sold_abc, profit_abc FROM product_abc WHERE id = %s", (row[0],))
                row2 = cursor.fetchone()

                if row2 is not None:
                    sold_abc = row2[0]
                    profit_abc = row2[1]

                    if sold_abc == 'A' and profit_abc == 'A':
                        top_products = 'AA'
                    else:
                        top_products = ''
                    cursor.execute(f"UPDATE product_abc SET top_products = %s WHERE id = %s", (top_products, row[0]))
                    conn.commit()
                else:
                    return False
            conn.commit()
            cursor.close()
            conn.close()
            return True
        else:
            pass
            conn.commit()
            cursor.close()
            conn.close()
            return True
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        if conn:
            conn.close()
        if cursor:
            cursor.close()

async def upsert_all(year):
    try:
        catalog_list = await get_product_catalogs(year)
        if catalog_list:
                await upsert_product_abc_part1(catalog_list, year)
                await upsert_product_abc_part2(year)
                await upsert_product_abc_part3(year)
                await upsert_product_abc_part4(year)
                await upsert_product_abc_part5(year)
                await upsert_product_abc_part6(year)
                await upsert_product_abc_part7(year)
                return True
        else:
            return False
    except Exception as e:
        print(f"Error: {e}")
        return False