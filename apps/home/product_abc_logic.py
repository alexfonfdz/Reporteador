from core.settings import ENV_PSQL_NAME, ENV_PSQL_USER, ENV_PSQL_PASSWORD, ENV_PSQL_HOST, ENV_PSQL_PORT, ENV_PSQL_DB_SCHEMA, ENV_MYSQL_HOST, ENV_MYSQL_PORT, ENV_MYSQL_NAME, ENV_MYSQL_USER, ENV_MYSQL_PASSWORD
import psycopg2 as p
# import mysql.connector as m
import mysql.connector as m
import datetime

async def upsert_families():
    try:
        conn = p.connect(dbname= ENV_PSQL_NAME, user=ENV_PSQL_USER, host=ENV_PSQL_HOST, password=ENV_PSQL_PASSWORD, port=ENV_PSQL_PORT)
        cursor = conn.cursor()
        cursor.execute(f"SELECT nombre FROM {ENV_PSQL_DB_SCHEMA}.admintotal_linea")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD, database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)
        cursor = conn.cursor()
        cursor.execute("SELECT id, name FROM family")
        rows2 = cursor.fetchall()
        for row in rows:
            if row[0] not in [r[1] for r in rows2]:
                cursor.execute(f"INSERT INTO family (name) VALUES ('%s')", (row[0],))
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

async def upsert_subfamilies():
    try:
        conn = p.connect(dbname= ENV_PSQL_NAME, user=ENV_PSQL_USER, host=ENV_PSQL_HOST, password=ENV_PSQL_PASSWORD, port=ENV_PSQL_PORT)
        cursor = conn.cursor()
        cursor.execute(f"SELECT nombre FROM {ENV_PSQL_DB_SCHEMA}.admintotal_sublinea")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD, database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)
        cursor = conn.cursor()
        cursor.execute("SELECT id, name FROM subfamily")
        rows2 = cursor.fetchall()
        for row in rows:
            if row[0] not in [r[1] for r in rows2]:
                cursor.execute(f"INSERT INTO subfamily (name) VALUES ('%s')", (row[0],))
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

async def upsert_products():
    try:
        conn = p.connect(dbname= ENV_PSQL_NAME, user=ENV_PSQL_USER, host=ENV_PSQL_HOST, password=ENV_PSQL_PASSWORD, port=ENV_PSQL_PORT)
        cursor = conn.cursor()
        cursor.execute(f"""
                       SELECT p.descripcion, p.codigo, p.id, sl.nombre, l.nombre, p.activo FROM {ENV_PSQL_DB_SCHEMA}.admintotal_producto as p 
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
                cursor.execute(f"INSERT INTO product (description, code, id_admin, family_id, subfamily_id) VALUES ('%s', '%s', %s, (SELECT id FROM family WHERE name = '%s'), (SELECT id FROM subfamily WHERE name = '%s'))", (row[0], row[1], row[2], row[4], row[3]))
            elif row[5] == True:
                cursor.execute(f"UPDATE product SET description = '%s', family_id = (SELECT id FROM family WHERE name = '%s'), subfamily_id = (SELECT id FROM subfamily WHERE name = '%s') WHERE code = '%s'", (row[0], row[4], row[3], row[1]))
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

async def upsert_product_catalogs(product_name, catalog_description, year):
    try:
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD, database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM product WHERE code = %s", (product_name,))
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
        if row is None and year < actual_year:
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
                    cursor.execute(f"SELECT id FROM family WHERE name = '%s'", (row[0],))
                    family_id = cursor.fetchone()
                    if family_id is None:
                        return False
                    family_id = family_id[0]
                    cursor.execute(f"SELECT id FROM subfamily WHERE name = '%s'", (row[1],))
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
                    cursor.execute(f"SELECT id FROM family WHERE name = '%s'", (row[0],))
                    family_id = cursor.fetchone()
                    if family_id is None:
                        return False
                    family_id = family_id[0]
                    cursor.execute(f"SELECT id FROM subfamily WHERE name = '%s'", (row[1],))
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
        if year < actual_year:
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
                                        WHERE sub.rn = 1;""", (year, product_tuple))
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