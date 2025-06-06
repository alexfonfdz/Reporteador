from core.settings import ENV_PSQL_NAME, ENV_PSQL_USER, ENV_PSQL_PASSWORD, ENV_PSQL_HOST, ENV_PSQL_PORT, ENV_PSQL_DB_SCHEMA, ENV_MYSQL_HOST, ENV_MYSQL_PORT, ENV_MYSQL_NAME, ENV_MYSQL_USER, ENV_MYSQL_PASSWORD
import psycopg2 as p
import mysql.connector as m
import datetime
import pandas as pd

"""
  La función 'upsert_families' realiza una sincronización de datos entre dos bases de 
  datos (PostgreSQL y MySQL) para insertar registros faltantes en la tabla family de MySQL.

    1. Se conecta a la base de datos PostgreSQL y se obtienen los nombres e IDs de la tabla admintotal_linea.
    2. Se conecta a la base de datos MySQL y se obtienen los nombres e IDs de la tabla family.
    3. Se compara la información de ambas tablas:
         - Si un nombre de familia de PostgreSQL no existe in MySQL, se inserta en la tabla family de MySQL.
         - Si ya existe, no se realiza ninguna acción.
    4. Se cierra la conexión a ambas bases de datos.
    5. Se imprime un mensaje indicando que las familias han sido insertadas correctamente.
"""
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

"""
  La función 'upsert_subfamilies' realiza una sincronización de datos entre dos bases de
  datos (PostgreSQL y MySQL) para insertar registros faltantes en la tabla subfamily de MySQL.

    1. Se conecta a la base de datos PostgreSQL y se obtienen los nombres e IDs de la tabla admintotal_sublinea.
    2. Se conecta a la base de datos MySQL y se obtienen los nombres e IDs de la tabla subfamily.
    3. Se compara la información de ambas tablas:
        - Si un nombre de subfamilia de PostgreSQL no existe en MySQL, se inserta en la tabla subfamily de MySQL.
        - Si ya existe, no se realiza ninguna acción.
    4. Se cierra la conexión a ambas bases de datos.
    5. Se imprime un mensaje indicando que las subfamilias han sido insertadas correctamente.
"""
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

"""
  La función 'upsert_products' realiza una sincronización de datos entre dos bases de
  datos (PostgreSQL y MySQL) para insertar o actualizar registros en la tabla product de MySQL.
    
    1. Se conecta a la base de datos PostgreSQL y se obtienen los detalles de los productos,
       incluyendo descripción, código, ID, nombre de sublínea y línea, estado activo y los IDs de
       sublínea y línea.
    2. Se conecta a la base de datos MySQL y se obtienen los códigos y IDs de los productos existentes
       en la tabla product.
    3. Se compara la información de ambas tablas:
        - Si un código de producto de PostgreSQL no existe en MySQL, se inserta en la tabla products de MySQL
          junto con el ID de la subfamilia y familia correspondientes.
        - Si ya existe y el producto está activo, se actualiza la descripción, la familia y la subfamilia
          en la tabla product de MySQL.
    4. Se cierra la conexión a ambas bases de datos.
    5. Se imprime un mensaje indicando que los productos han sido insertados correctamente.
"""
async def upsert_products():
    try:
        conn = p.connect(dbname= ENV_PSQL_NAME, user=ENV_PSQL_USER, host=ENV_PSQL_HOST, password=ENV_PSQL_PASSWORD, port=ENV_PSQL_PORT)
        cursor = conn.cursor()
        cursor.execute(f"""
                       SELECT p.descripcion, p.codigo, p.id, sl.nombre, l.nombre, p.activo, p.sublinea_id, p.linea_id FROM {ENV_PSQL_DB_SCHEMA}.admintotal_producto as p 
                       INNER JOIN {ENV_PSQL_DB_SCHEMA}.admintotal_sublinea as sl ON p.sublinea_id = sl.id 
                       INNER JOIN {ENV_PSQL_DB_SCHEMA}.admintotal_linea as l ON p.linea_id = l.id
                       """)
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

"""
  La función 'upsert_catalogs' realiza la inserción de un nuevo catálogo en la base de datos MySQL.

    1. Verifica si la descripción proporcionada es valida (no nula, no vacía y no contiene valores
       no deseados como 'N/A' o '0').
    2. Se conecta a la base de datos MySQL y verifica si ya existe un catálogo con la misma descripción.
        - Si no existe, se inserta un nuevo registro.        
    3. Se cierra la conexión a la base de datos.    
"""
async def upsert_catalogs(description):
    try:                
        print(f"Catalogo: {description}")

        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD, database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)
        cursor = conn.cursor()

        # Verificar si la descripción es válida
        if description[0] not in ['N/A', '0', 0, None, ''] and description[1] not in ['N/A', '0', 0, None, ''] and description[2] not in ['N/A', '0', 0, None, '']:

            # Verificar si ya existe un catálogo con la misma descripción
            cursor.execute("SELECT id FROM catalog WHERE description = %s", (description[0],))
            row = cursor.fetchone()                                    

            if pd.isna(description[1]):                
                description[1] = None
            
            if pd.isna(description[2]):
                description[2] = None
            
            # Si no existe, insertar un nuevo catálogo
            if row is None:
                cursor.execute(
                       "INSERT INTO catalog (description, family, subfamily) VALUES (%s, %s, %s)",
                        (description[0], description[1], description[2],)
                )
        else:
            raise ValueError("El formato de la descripcion no es valido")
                                                    
        conn.commit()
        cursor.close()
        conn.close() 
    except Exception as e:
        print(f"Error: {e}")        
    finally:
        if conn:
            conn.close()
        if cursor:
            cursor.close()

"""
  La función 'upsert_product_catalogs' realiza la inserción o actualización de registros 
  en la tabla product_catalog de MySQL.

    1. Se conecta a la base de datos MySQL.
    2. Busca el ID del producto utilizando el código del producto proporcionado.    
        - Si no se encuentra el producto, retorna False.
    3. Busca el ID del catálogo utilizando la descripción del catálogo proporcionada.
        - Si no se encuentra el catálogo, retorna False.
    4. Busca un registro en la tabla product_catalog utilizando el ID del producto y el ID
       del catálogo.
        - Si se encuentra un producto, pero el catálogo asociado es diferente o el año es 
          diferente, se acualiza el registro con el nuevo catálogo.
        - Si no se encuentra un registro para el producto, el catálogo y el año, se inserta
          un nuevo registro en la tabla product_catalog.
    5. Se cierra la conexión a la base de datos.    
"""
async def upsert_product_catalogs(product_code, catalog_description, year):
    try:
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD, database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM product WHERE code = %s", (product_code,))
        product_id = cursor.fetchone()
        if product_id is None:
            print(f"Producto no encontrado: {product_code}")
            return False
        product_id = product_id[0]

        cursor.execute("SELECT id FROM catalog WHERE description = %s", (catalog_description,))
        catalog_id = cursor.fetchone()
        if catalog_id is None:
            print(f"Catálogo no encontrado: {catalog_description}")
            return False
        catalog_id = catalog_id[0]

        cursor.execute("SELECT id, product_id, catalog_id, add_year FROM product_catalog WHERE product_id = %s", (product_id,))
        row = cursor.fetchone()
        if row is not None:
            print(f"Registro encontrado en product_catalog: {row}")
            if row[2] != catalog_id or row[3] == year:
                print("Actualizando registro existente.")
                cursor.execute("UPDATE product_catalog SET catalog_id = %s, last_update = NOW() WHERE product_id = %s AND add_year = %s", (catalog_id, product_id, year))
            elif row[1] == product_id and row[2] == catalog_id and row[3] == year:
                pass
            else:
                print("Insertando nuevo registro.")
                cursor.execute("INSERT INTO product_catalog (product_id, catalog_id, add_year, last_update) VALUES (%s, %s, %s, NOW())", (product_id, catalog_id, year))
        else:
            print("Insertando nuevo registro porque no existe en product_catalog.")
            cursor.execute("INSERT INTO product_catalog (product_id, catalog_id, add_year, last_update) VALUES (%s, %s, %s, NOW())", (product_id, catalog_id, year))
        conn.commit()
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        if conn:
            conn.close()
        if cursor:
            cursor.close()

"""
  La función 'get_product_catalogs' obtiene una lista de catálogos y los productos asociados 
  para un año específico o años anteriores.

    1. Se conecta a la base de datos MySQL.
    2. Ejecuta una consulta SQL para obtener el ID del código del producto, el ID del catálogo
       y el año desde la tabla product_catalog. La consulta selecciona el último catálogo disponible
       para cada producto en el año seleccionado o años anteriores.
    3. Los resultados se agrupan en un diccionario donde la clave es el ID del catálogo y el valor es
       una lista de códigos de productos asociados.
    4. Convierte el diccionario en una lista de tuplas, donde cada tupla contiene el ID del catálogo y
       una cadena con los códigos de productos asociados separados por comas.
    5. Retorna la lista de tuplas y cierra la conexión a la base de datos.
"""
async def get_product_catalogs(year):
    try:
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD, database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)
        cursor = conn.cursor()
        # Tomar el ultimo catalogo de cada producto del año seleccionado para atras en caso de que no haya un catalogo para el año seleccionado
        cursor.execute(f"""
                        SELECT p.code, 
                               c.id, 
                               pc.add_year 
                        FROM product_catalog as pc 
                            INNER JOIN catalog as c 
                                ON pc.catalog_id = c.id 
                            INNER JOIN product as p 
                                ON pc.product_id = p.id 
                        WHERE pc.add_year <= %s 
                            AND pc.add_year = (SELECT MAX(add_year) 
                                               FROM product_catalog 
                                               WHERE product_id = p.id 
                                                    AND add_year <= %s)
                       """, (year, year))
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

"""
  La función 'upsert_product_abc_part0' inserta todos los catálogos de la tabla product_catalog
  en la tabla product_abc de MySQL. Se le proporciona un año y un ID de empresa como parámetros.

  Además de insertar los catálogos, también inserta las familias y subfamilias correspondientes.  
"""
async def upsert_product_abc_part0(year, enterprise="marw"):
    try:
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD, database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)
        cursor = conn.cursor()
        cursor.execute("""
                        SELECT 
                            c.id 
                        FROM catalog c
                        INNER JOIN product_catalog pc
                            ON pc.catalog_id = c.id
                        WHERE pc.add_year <= %s
                        GROUP BY c.id
                       """, (year,))
        rows = cursor.fetchall()

        for row in rows:            
            # Verificar si ya existe un registro con estos IDs
            cursor.execute("""
                            SELECT id 
                            FROM product_abc 
                            WHERE catalog_id = %s                                 
                                AND year = %s
                                AND assigned_company = %s
            """, (row[0], year, enterprise))
            existing_record = cursor.fetchone()

            # Si no existe, insertar el registro
            if existing_record is None:
                cursor.execute("""
                    INSERT INTO product_abc (catalog_id, year, assigned_company, last_update) 
                    VALUES (%s, %s, %s, NOW())
                """, (row[0], year, enterprise))

        conn.commit()
        cursor.close()
        conn.close()
        print("Registros insertados correctamente en product_abc.")
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        if conn:
            conn.close()
        if cursor:
            cursor.close()

"""
  La función 'upsert_product_abc_part1' inserta o actualiza los registros en la tabla
  product_abc de MySQL para un año específico.

    1. Se conecta a la base de datos MySQL y verifica si ya existen registros para el año
       proporcionado. Si no existen registros o el último mes actualizado es menor a un umbral,
       se procede a insertar o actualizar los registros.
    2. Para cada catálogo en la lista de catálogos, se conecta a la base de datos PostgreSQL y
       ejecuta una consulta SQL para obtener el total de importe, utilidad y unidades vendidas
       agrupados por familia y subfamilia.
    3. Con los resultados obtenidos, busca los IDs de las familias y subfamilias en la base de 
       datos MySQL:
        - Si no existe un registro en product_abc para el catálogo, familia, subfamilia y año
          especificado, se inserta un nuevo registro con los datos obtenidos.
        - Si ya existe un registro, se actualizan los valores de importe total, utilidad y
          unidades vendidas. 
    4. Si el año es el año actual, se repite el proceso de inserción o actualización de registros
    5. Finalmente, se cierra la conexión a la base de datos.
"""
async def upsert_product_abc_part1(catalog_list, year, enterprise = "marw"):
    try:
        actual_year = datetime.datetime.now().year
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD, database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)
        cursor = conn.cursor(buffered=True)
        row_year = None
        cursor.execute("SELECT year FROM product_abc WHERE year = %s", (year,))        
        row_year_sql = cursor.fetchone()        
        if row_year_sql is not None:
            row_year = row_year_sql[0]        

        if row_year is None and year < actual_year or (int(year) + 1 == actual_year):
            conn_pg = p.connect(dbname=ENV_PSQL_NAME, user=ENV_PSQL_USER, host=ENV_PSQL_HOST, password=ENV_PSQL_PASSWORD, port=ENV_PSQL_PORT)
            cursor_pg = conn_pg.cursor()
            for catalog in catalog_list:
                cursor_pg.execute(f"""
                                  SELECT
                                    ROUND(SUM(sub.TOTAL_IMPORTE),2) AS TOTAL_IMPORTE,
                                    ROUND(SUM(sub.TOTAL_UTILIDAD),2) AS TOTAL_UTILIDAD,
                                    ROUND(SUM(sub.UNIDADES_VENDIDAS),2) AS UNIDADES_VENDIDAS
                                    FROM (SELECT                                        
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
                                  ) sub ;""", (year, tuple(catalog[1].split(', '))))
                rows = cursor_pg.fetchone()

                if rows is not None:                                       
                    total_amount = rows[0]
                    profit = rows[1]
                    units_sold = rows[2]
                    
                    # Verificar si ya existe un registro con estos valores en product_abc
                    cursor.execute(f"""
                                    SELECT id 
                                    FROM product_abc
                                    WHERE catalog_id = %s
                                        AND year = %s
                                        AND assigned_company = %s
                                        AND (total_amount IS NULL OR profit IS NULL OR units_sold IS NULL)                                   
                                   """, (catalog[0], year, enterprise))
                    existing_record = cursor.fetchone()

                    if existing_record is None:
                        pass
                    else:
                        # Actualizar registro existente
                        cursor.execute(f"""UPDATE product_abc SET total_amount = %s, profit = %s, units_sold = %s 
                                            WHERE catalog_id = %s AND year = %s AND assigned_company = %s""", (total_amount, profit, units_sold, catalog[0], year, enterprise))
                        print("UPDATE al registro existente en product_abc")
                        print("Catalogo: ", catalog[0])
                        conn.commit()
            cursor_pg.close()
            conn_pg.close() 
        elif year == actual_year:
            conn_pg = p.connect(dbname=ENV_PSQL_NAME, user=ENV_PSQL_USER, host=ENV_PSQL_HOST, password=ENV_PSQL_PASSWORD, port=ENV_PSQL_PORT)
            cursor_pg = conn_pg.cursor()
            for catalog in catalog_list:
                cursor_pg.execute(f"""
                                  SELECT
                                    ROUND(SUM(sub.TOTAL_IMPORTE),2) AS TOTAL_IMPORTE,
                                    ROUND(SUM(sub.TOTAL_UTILIDAD),2) AS TOTAL_UTILIDAD,
                                    ROUND(SUM(sub.UNIDADES_VENDIDAS),2) AS UNIDADES_VENDIDAS
                                  FROM (SELECT
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
                                  ) sub;""", (year, tuple(catalog[1].split(', '))))
                rows = cursor_pg.fetchone()

                if rows is not None:                    
                    total_amount = rows[0]
                    profit = rows[1]
                    units_sold = rows[2]

                    # Verificar si ya existe un registro con estos valores
                    cursor.execute(f"""
                                    SELECT id 
                                    FROM product_abc 
                                    WHERE catalog_id = %s                                         
                                        AND year = %s
                                        AND assigned_company = %s                                   
                                   """, (catalog[0], year, enterprise))
                    existing_record = cursor.fetchone()

                    if existing_record is None:                        
                        pass
                    else:
                        # Actualizar registro existente
                        cursor.execute(f"""UPDATE product_abc SET total_amount = %s, profit = %s, units_sold = %s 
                                            WHERE catalog_id = %s AND year = %s AND assigned_company = %s""", (total_amount, profit, units_sold, catalog[0], year, enterprise))
                        conn.commit()
            cursor_pg.close()
            conn_pg.close()
        else:
            pass                 
        cursor.close()
        conn.close()        
        return True
        
    except m.Error as e:
        print(f"Error MySQL: {e}")
        return False    
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if cursor_pg:
            cursor_pg.close()
        if conn:
            conn.close()
        if conn_pg:
            conn_pg.close()


async def upsert_product_abc_part2(year, enterprise = "marw"):
    try:
        actual_year = datetime.datetime.now().year
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD, database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)
        cursor = conn.cursor(buffered=True)
        last_month_update = 0
        cursor.execute("SELECT MAX(last_update) FROM product_abc WHERE year = %s AND assigned_company = %s", (year, enterprise))
        row = cursor.fetchone()
        if row is not None and row[0] is not None:
            last_month_update = row[0].month
        if int(year) < actual_year or (int(year) +1 == actual_year and last_month_update < 6):
            cursor.execute("SELECT id, catalog_id FROM product_abc WHERE year = %s AND (inventory_close_u IS NULL OR inventory_close_p IS NULL) AND assigned_company = %s", (year, enterprise))
            rows = cursor.fetchall()

            for row in rows:
                row_catalog_id = row[1]
                cursor.execute(f"SELECT product_id FROM product_catalog WHERE catalog_id = %s AND add_year <= %s AND add_year = (SELECT MAX(add_year) FROM product_catalog WHERE catalog_id = %s AND add_year <= %s)", (row_catalog_id, year, row_catalog_id, year))
                rows2 = cursor.fetchall()
                rows2_tuple = tuple([r[0] for r in rows2])                

                product_tuple = None;
                
                if rows2_tuple:                                                            
                    cursor.execute(f"SELECT code FROM product WHERE id IN %s", (rows2_tuple))                                        
                    rows3 = cursor.fetchall()

                    product_tuple = tuple([r[0] for r in rows3])                                        

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
                        print("Catalogo: ", row_catalog_id)                        
                        cursor.execute(
                        "UPDATE product_abc SET inventory_close_u = %s, inventory_close_p = %s WHERE id = %s AND year = %s AND assigned_company = %s",
                        (row3[0], row3[1], row[0], year, enterprise))
            conn.commit()
            cursor.close()
            conn.close()
            return True
        elif year == actual_year:
            cursor.execute("SELECT id, catalog_id FROM product_abc WHERE year = %s AND assigned_company = %s", (year, enterprise))
            rows = cursor.fetchall()

            for row in rows:
                row_catalog_id = row[1]
                cursor.execute(f"SELECT product_id FROM product_catalog WHERE catalog_id = %s AND add_year <= %s AND add_year = (SELECT MAX(add_year) FROM product_catalog WHERE catalog_id = %s AND add_year <= %s)", (row_catalog_id, year, row_catalog_id, year))
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
                        cursor.execute(f"SELECT id FROM product_abc WHERE catalog_id = %s AND year = %s AND assigned_company = %s", (row_catalog_id, year, enterprise))
                        row2 = cursor.fetchone()
                        if row2 is None:
                            cursor.execute(f"INSERT INTO product_abc (catalog_id, inventory_close_u, inventory_close_p, year, enterprise) VALUES (%s, %s, %s, %s, %s)", (row_catalog_id, row3[0], row3[1], year, enterprise))
                        else:
                            cursor.execute(
                            "UPDATE product_abc SET inventory_close_u = %s, inventory_close_p = %s WHERE id = %s AND year = %s AND assigned_company = %s",
                            (row3[0], row3[1], row2[0], year, enterprise))
            conn.commit()
            cursor.close()
            conn.close()
        else:
            pass                 
        cursor.close()
        conn.close()        
        return True
        
    except m.Error as e:
        print(f"Error MySQL: {e}")
        return False    
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if cursor_pg:
            cursor_pg.close()
        if conn:
            conn.close()
        if conn_pg:
            conn_pg.close()


async def upsert_product_abc_part3(year, enterprise="marw"):
    try:
        actual_year = datetime.datetime.now().year
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD,
                         database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, last_update, catalog_id FROM product_abc 
            WHERE year = %s AND assigned_company = %s
        """, (year, enterprise))
        rows = cursor.fetchall()

        for row_id, last_update, catalog_id in rows:
            if last_update and last_update.year != int(year):
                continue

            cursor.execute("""
                SELECT product_id FROM product_catalog 
                WHERE catalog_id = %s AND add_year <= %s 
                AND add_year = (SELECT MAX(add_year) FROM product_catalog WHERE catalog_id = %s AND add_year <= %s)
            """, (catalog_id, year, catalog_id, year))
            product_ids = [r[0] for r in cursor.fetchall()]
            if not product_ids:
                continue

            cursor.execute("SELECT code FROM product WHERE id IN %s", (tuple(product_ids),))
            product_codes = [r[0] for r in cursor.fetchall()]
            if not product_codes:
                continue

            conn_pg = p.connect(dbname=ENV_PSQL_NAME, user=ENV_PSQL_USER, host=ENV_PSQL_HOST,
                                password=ENV_PSQL_PASSWORD, port=ENV_PSQL_PORT)
            cursor_pg = conn_pg.cursor()

            for month in range(1, 13):
                cursor_pg.execute("""
                    SELECT
                        ROUND(SUM(sub.existencias), 2),
                        ROUND(SUM(sub.existencias_pesos), 2)
                    FROM (
                        SELECT
                            ROUND(SUM(CASE WHEN m_inv.es_entrada THEN md_inv.cantidad WHEN m_inv.es_salida THEN -md_inv.cantidad END), 5) AS existencias,
                            ROUND(SUM(CASE WHEN m_inv.es_entrada THEN md_inv.cantidad * md_inv.costo_venta WHEN m_inv.es_salida THEN -md_inv.cantidad * md_inv.costo_venta END), 5) AS existencias_pesos
                        FROM marw.admintotal_movimientodetalle md_inv
                        INNER JOIN marw.admintotal_movimiento m_inv ON m_inv.poliza_ptr_id = md_inv.movimiento_id
                        INNER JOIN marw.admintotal_poliza p_inv_lookup ON m_inv.poliza_ptr_id = p_inv_lookup.id
                        INNER JOIN marw.admintotal_producto prod_inv ON md_inv.producto_id = prod_inv.id
                        WHERE m_inv.pendiente = true
                          AND EXTRACT(YEAR FROM TIMEZONE('America/Mexico_City', p_inv_lookup.fecha)) = %s
                          AND EXTRACT(MONTH FROM TIMEZONE('America/Mexico_City', p_inv_lookup.fecha)) = %s
                          AND prod_inv.codigo IN %s
                        GROUP BY m_inv.almacen_id, md_inv.producto_id
                    ) sub
                """, (year, month, tuple(product_codes)))

                result = cursor_pg.fetchone()
                if result:
                    inventory_u, inventory_p = result
                    u_col = f"inventory_close_u_{datetime.date(1900, month, 1).strftime('%B').lower()}"
                    p_col = f"inventory_close_p_{datetime.date(1900, month, 1).strftime('%B').lower()}"
                    cursor.execute(f"""
                    UPDATE product_abc SET {u_col} = %s, {p_col} = %s, last_update = NOW()
                    WHERE id = %s AND year = %s AND assigned_company = %s
                    """, (inventory_u, inventory_p, row_id, year, enterprise))
                    conn.commit()

            cursor_pg.close()
            conn_pg.close()

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"Error: {e}")
        return False

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

"""
  La función 'upsert_product_abc_part4' inserta todos los catálogos de la tabla product_catalog
  en la tabla product_abc de MySQL. Se le proporciona un año y un ID de empresa como parámetros.

  Además de insertar los catálogos, también inserta las familias y subfamilias correspondientes.  
"""
async def upsert_product_abc_part4(year, enterprise="marw"):
    try:
        actual_year = datetime.datetime.now().year
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD,
                         database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)
        cursor = conn.cursor()

        cursor.execute("SELECT id, last_update FROM product_abc WHERE year = %s AND assigned_company = %s", (year, enterprise))
        rows = cursor.fetchall()

        column_months = [
            'inventory_close_u_january', 'inventory_close_p_january', 'inventory_close_u_february', 'inventory_close_p_february',
            'inventory_close_u_march', 'inventory_close_p_march', 'inventory_close_u_april', 'inventory_close_p_april',
            'inventory_close_u_may', 'inventory_close_p_may', 'inventory_close_u_june', 'inventory_close_p_june',
            'inventory_close_u_july', 'inventory_close_p_july', 'inventory_close_u_august', 'inventory_close_p_august',
            'inventory_close_u_september', 'inventory_close_p_september', 'inventory_close_u_october', 'inventory_close_p_october',
            'inventory_close_u_november', 'inventory_close_p_november', 'inventory_close_u_december', 'inventory_close_p_december']

        for row_id, last_update in rows:
            if last_update and last_update.year != int(year):
                continue

            cursor.execute(f"SELECT {', '.join(column_months)} FROM product_abc WHERE id = %s", (row_id,))
            row2 = cursor.fetchone()
            if row2 is None:
                continue

            total_inventory_close_u = sum(row2[idx] if row2[idx] else 0 for idx in range(0, len(column_months), 2))
            total_inventory_close_p = sum(row2[idx] if row2[idx] else 0 for idx in range(1, len(column_months), 2))

            cursor.execute("""
            UPDATE product_abc SET inventory_average_u = %s, inventory_average_p = %s, last_update = NOW()
            WHERE id = %s AND year = %s AND assigned_company = %s
            """, (total_inventory_close_u, total_inventory_close_p, row_id, year, enterprise))
            conn.commit()

            cursor.execute("""
                SELECT total_amount, profit, inventory_close_u, inventory_average_u, inventory_average_p
                FROM product_abc WHERE id = %s
            """, (row_id,))
            row3 = cursor.fetchone()
            if row3 is None:
                continue

            total_amount, profit, inventory_close_u, avg_u, avg_p = row3
            profit_average_month = profit / 12 if profit else 0
            monthly_roi = (profit_average_month / avg_p) * 100 if avg_p else 0
            sold_average_month = total_amount / 12 if total_amount else 0
            average_selling_cost = (total_amount - profit) / 12 if profit else 0
            inventory_days = (inventory_close_u / sold_average_month) * 30 if sold_average_month else 0

            cursor.execute("""
            UPDATE product_abc SET monthly_roi = %s, sold_average_month = %s, profit_average_month = %s,
            actual_inventory = %s, average_selling_cost = %s, inventory_days = %s
            WHERE id = %s AND year = %s AND assigned_company = %s
            """, (monthly_roi, sold_average_month, profit_average_month, inventory_close_u,
            average_selling_cost, inventory_days, row_id, year, enterprise))
            conn.commit()

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"Error: {e}")
        return False

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()


"""
  La función 'upsert_product_abc_part5' inserta todos los catálogos de la tabla product_catalog
  en la tabla product_abc de MySQL. Se le proporciona un año y un ID de empresa como parámetros.

  Además de insertar los catálogos, también inserta las familias y subfamilias correspondientes.  
"""
async def upsert_product_abc_part5(year, enterprise="marw"):
    try:
        actual_year = datetime.datetime.now().year
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD,
                         database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)
        cursor = conn.cursor()

        cursor.execute("SELECT id, last_update FROM product_abc WHERE year = %s AND assigned_company = %s ORDER BY total_amount DESC", (year, enterprise))
        rows = cursor.fetchall()

        cursor.execute("SELECT SUM(total_amount) FROM product_abc WHERE year = %s AND assigned_company = %s", (year, enterprise))
        total_general_amount = cursor.fetchone()[0] or 0

        acc_sales_percentage = 0
        for row_id, last_update in rows:
            if last_update and last_update.year != int(year):
                continue

            cursor.execute("SELECT total_amount FROM product_abc WHERE id = %s", (row_id,))
            total_amount = cursor.fetchone()[0] or 0

            sales_percentage = (total_amount / total_general_amount) * 100 if total_general_amount else 0
            acc_sales_percentage += sales_percentage

            sold_abc = 'A' if acc_sales_percentage <= 80 else 'B' if acc_sales_percentage <= 95 else 'C'

            cursor.execute("""
            UPDATE product_abc SET sales_percentage = %s, acc_sales_percentage = %s, sold_abc = %s, last_update = NOW()
            WHERE id = %s AND year = %s AND assigned_company = %s
            """, (sales_percentage, acc_sales_percentage, sold_abc, row_id, year, enterprise))
            conn.commit()

        cursor.close()
        conn.close()
        print("Clasificación ABC de ventas actualizada correctamente.")
        return True

    except Exception as e:
        print(f"Error: {e}")
        return False

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()


"""
  La función 'upsert_product_abc_part6' inserta todos los catálogos de la tabla product_catalog
  en la tabla product_abc de MySQL. Se le proporciona un año y un ID de empresa como parámetros.

  Además de insertar los catálogos, también inserta las familias y subfamilias correspondientes.  
"""
async def upsert_product_abc_part6(year, enterprise="marw"):
    try:
        actual_year = datetime.datetime.now().year
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD,
                         database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)
        cursor = conn.cursor()

        cursor.execute("SELECT id, last_update FROM product_abc WHERE year = %s AND assigned_company = %s ORDER BY profit DESC", (year, enterprise))
        rows = cursor.fetchall()

        cursor.execute("SELECT SUM(profit) FROM product_abc WHERE year = %s AND assigned_company = %s", (year, enterprise))
        total_general_profit = cursor.fetchone()[0] or 0

        acc_profit_percentage = 0
        for row_id, last_update in rows:
            if last_update and last_update.year != int(year):
                continue

            cursor.execute("SELECT profit FROM product_abc WHERE id = %s", (row_id,))
            profit = cursor.fetchone()[0] or 0

            profit_percentage = (profit / total_general_profit) * 100 if total_general_profit else 0
            acc_profit_percentage += profit_percentage

            profit_abc = 'A' if acc_profit_percentage <= 80 else 'B' if acc_profit_percentage <= 95 else 'C'

            cursor.execute("""
            UPDATE product_abc SET profit_percentage = %s, acc_profit_percentage = %s, profit_abc = %s, last_update = NOW()
            WHERE id = %s AND year = %s AND assigned_company = %s
            """, (profit_percentage, acc_profit_percentage, profit_abc, row_id, year, enterprise))
            conn.commit()

        cursor.close()
        conn.close()
        print("Clasificación ABC de utilidad actualizada correctamente.")
        return True

    except Exception as e:
        print(f"Error: {e}")
        return False

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()


"""
  La función 'upsert_product_abc_part7' inserta todos los catálogos de la tabla product_catalog
  en la tabla product_abc de MySQL. Se le proporciona un año y un ID de empresa como parámetros.

  Además de insertar los catálogos, también inserta las familias y subfamilias correspondientes.  
"""
async def upsert_product_abc_part7(year, enterprise="marw"):
    try:
        actual_year = datetime.datetime.now().year
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD,
                         database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)
        cursor = conn.cursor()

        cursor.execute("SELECT id, last_update FROM product_abc WHERE year = %s AND assigned_company = %s", (year, enterprise))
        rows = cursor.fetchall()

        for row_id, last_update in rows:
            if last_update and last_update.year != int(year):
                continue

            cursor.execute("SELECT sold_abc, profit_abc FROM product_abc WHERE id = %s", (row_id,))
            result = cursor.fetchone()
            if not result:
                continue

            sold_abc, profit_abc = result
            top_products = 'AA' if sold_abc == 'A' and profit_abc == 'A' else ''

            cursor.execute("""
            UPDATE product_abc SET top_products = %s, last_update = NOW() WHERE id = %s AND year = %s AND assigned_company = %s
            """, (top_products, row_id, year, enterprise))
            conn.commit()

        cursor.close()
        conn.close()
        print("Campo top_products actualizado correctamente.")
        return True

    except Exception as e:
        print(f"Error: {e}")
        return False

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()


async def upsert_all(year, enterprise):
    
    try:
        await upsert_product_abc_part0(year, enterprise)
        catalog_list = await get_product_catalogs(year)        
        if catalog_list:               
                await upsert_product_abc_part1(catalog_list, year, enterprise)        
                await upsert_product_abc_part2(year, enterprise)
                await upsert_product_abc_part3(year, enterprise)
                await upsert_product_abc_part4(year, enterprise)
                await upsert_product_abc_part5(year, enterprise)
                await upsert_product_abc_part6(year, enterprise)
                await upsert_product_abc_part7(year, enterprise)
                
                return True
        else:
            return False
    except Exception as e:
        print(f"Error: {e}")
        return False