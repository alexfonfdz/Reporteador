from django.conf import traceback
from django.test.testcases import asyncio
from core.settings import ENV_PSQL_NAME, ENV_PSQL_USER, ENV_PSQL_PASSWORD, ENV_PSQL_HOST, ENV_PSQL_PORT, ENV_PSQL_DB_SCHEMA, ENV_MYSQL_HOST, ENV_MYSQL_PORT, ENV_MYSQL_NAME, ENV_MYSQL_USER, ENV_MYSQL_PASSWORD
import psycopg2 as p
import mysql.connector as m
import datetime
import pandas as pd
import aiomysql as aiom
import asyncpg as aiop

"""
  La función 'upsert_families' realiza una sincronización de datos entre dos bases de 
  datos (PostgreSQL y MySQL) para insertar registros faltantes en la tabla family de MySQL.

    1. Se conecta a la base de datos PostgreSQL y se obtienen los nombres e IDs de la tabla admintotal_linea.
    2. Se conecta a la base de datos MySQL y se obtienen los nombres e IDs de la tabla family.
    3. Se compara la información de ambas tablas:
         - Si un nombre de familia de PostgreSQL no existe en MySQL, se inserta en la tabla family de MySQL.
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
        print(traceback.print_exc())
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
        print(traceback.print_exc())
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
    conn = cursor = conn_pg = cursor_pg = None
    try:
        conn_pg = p.connect(dbname= ENV_PSQL_NAME, user=ENV_PSQL_USER, host=ENV_PSQL_HOST, password=ENV_PSQL_PASSWORD, port=ENV_PSQL_PORT)
        cursor_pg = conn_pg.cursor()
        cursor_pg.execute(f"""
                       SELECT p.descripcion, p.codigo, p.id, sl.nombre, l.nombre, p.activo, p.sublinea_id, p.linea_id FROM {ENV_PSQL_DB_SCHEMA}.admintotal_producto as p 
                       INNER JOIN {ENV_PSQL_DB_SCHEMA}.admintotal_sublinea as sl ON p.sublinea_id = sl.id 
                       INNER JOIN {ENV_PSQL_DB_SCHEMA}.admintotal_linea as l ON p.linea_id = l.id
                       """)
        new_rows = cursor_pg.fetchall()
        cursor_pg.close()
        conn_pg.close()

        conn = await aiom.connect(
            db=ENV_MYSQL_NAME,
            user=ENV_MYSQL_USER,
            password=ENV_MYSQL_PASSWORD or "",
            host=ENV_MYSQL_HOST or "",
            port=int(ENV_MYSQL_PORT or "3306"),
        )

        async def upsert_job(conn, new_rows: list, existent_rows: set):

            cursor = await conn.cursor()

            insert_query = "INSERT INTO product (description, code, id_admin, family_id, subfamily_id) VALUES (%s, %s, %s, (SELECT id FROM family WHERE id_admin = %s), (SELECT id FROM subfamily WHERE id_admin = %s))"
            update_query = "UPDATE product SET description = %s, family_id = (SELECT id FROM family WHERE id_admin = %s), subfamily_id = (SELECT id FROM subfamily WHERE id_admin = %s) WHERE code = %s"
            insert_query_params = []
            update_query_params = []

            for row in new_rows:
                if row[1] not in existent_rows:                                
                    insert_query_params.append((row[0], row[1], row[2], row[7], row[6]))
                elif row[5] == True:
                    update_query_params.append((row[0], row[7], row[6], row[1]))
            try:
                if len(insert_query_params) > 0:
                    await cursor.executemany(insert_query, insert_query_params)
                if len(update_query_params) > 0:
                    await cursor.executemany(update_query, update_query_params)
            except Exception as err:
                print(f'Inserting product error:{err}')


                        
            await cursor.close()


        cursor = await conn.cursor()
        await cursor.execute("SELECT id, code FROM product")
        existent_rows = set(r[1] for r in await cursor.fetchall())

        page_content = 500
        pages_amount = len(new_rows) // page_content
        remainder = len(new_rows) % page_content

        for i in range(pages_amount):
                await upsert_job(conn, new_rows[i*page_content:(i+1)*page_content], existent_rows)

        if remainder:
                await upsert_job(conn, new_rows[pages_amount*page_content:], existent_rows)

        await conn.commit()


        print("Productos insertados correctamente")
        return True
    except Exception as e:
        print(f"Error: {e}")
        print(traceback.print_exc())
        return False
    finally:
        if conn:
            await conn.close()
        if cursor:
            await cursor.close()
        if conn_pg:
            conn_pg.close()
        if cursor_pg:
            cursor_pg.close()
"""
  La función 'upsert_catalogs' realiza la inserción de un nuevo conjunto de catálogos en la base de datos MySQL.

    1. Verifica si la descripción proporcionada es valida (no nula, no vacía y no contiene valores
       no deseados como 'N/A' o '0').
    2. Se conecta a la base de datos MySQL y verifica si ya existe un catálogo con la misma descripción.
        - Si no existe, se inserta un nuevo registro.        
    3. Se cierra la conexión a la base de datos.    
"""
async def upsert_catalogs(catalogs):
    conn =  cursor = None
    try:                

        conn = await aiom.connect(
            host=ENV_MYSQL_HOST or "localhost",
            user=ENV_MYSQL_USER or "user",
            password=ENV_MYSQL_PASSWORD or "password",
            db=ENV_MYSQL_NAME or "dbname",
            port=int(ENV_MYSQL_PORT or "3306")
        )

        invalid_catalog = ['N/A', '0', 0, None, '', ' ']

        async with conn.cursor() as cursor:
            for catalog in catalogs:
            # Verificar si la descripción es válida
                if catalog[0] in invalid_catalog and catalog[1] in invalid_catalog and catalog[2] in invalid_catalog:
                    raise ValueError("El formato de la descripcion no es valido")

                if pd.isna(catalog[1]):                
                    catalog[1] = None
        
                if pd.isna(catalog[2]):
                    catalog[2] = None


                await cursor.execute(
                    "SELECT id FROM catalog WHERE description = %s",
                    catalog[0]
                )

                existing_row = await cursor.fetchone()

                if existing_row:
                    continue

                await cursor.execute(
                    "INSERT INTO catalog (description, family, subfamily) VALUES (%s, %s, %s)",
                    (catalog[0], catalog[1], catalog[2],)
                )
                                                    
            await conn.commit()
            await cursor.close()


    except Exception as e:
        print(f"Error: {e}")        
        print(traceback.print_exc())


"""
  La función 'upsert_product_catalogs' realiza la inserción o actualización de registros 
  en la tabla product_catalog de MySQL.

    1. Se conecta a la base de datos MySQL.
    2. Busca el ID del producto utilizando el código del producto proporcionado.    
        - Si no se encuentra el producto, retorna False.
    3. Busca el ID del catálogo utilizando la descripción del catálogo proporcionado.
        - Si no se encuentra el catálogo, retorna False.
    4. Busca un registro en la tabla product_catalog utilizando el ID del producto y el ID
       del catálogo.
        - Si se encuentra un producto, pero el catálogo asociado es diferente o el año es 
          diferente, se acualiza el registro con el nuevo catálogo.
        - Si no se encuentra un registro para el producto, el catálogo y el año, se inserta
          un nuevo registro en la tabla product_catalog.
    5. Se cierra la conexión a la base de datos.    
"""
async def upsert_product_catalogs(product_catalog, year):
    conn = cursor = None
    try:
        conn = m.connect(
            host=ENV_MYSQL_HOST or "localhost",
            user=ENV_MYSQL_USER or "user",
            password=ENV_MYSQL_PASSWORD or "password",
            database=ENV_MYSQL_NAME or "dbname",
            port=int(ENV_MYSQL_PORT or "3306")
        )
        cursor = conn.cursor()
        queries_params = [(year, product['Codigo'], product['Catalogo']) for product in product_catalog]
        query = """
                    INSERT IGNORE INTO 
                    product_catalog (product_id, catalog_id, add_year, last_update) 
                        SELECT p.id, c.id, %s, NOW()
                            FROM 
                                product AS p JOIN catalog AS c ON 1=1
                            WHERE
                                p.code = %s AND c.description = %s
                            LIMIT 1
                """

        cursor.executemany(query, queries_params)

        conn.commit()
        return True
    except Exception as e:
        print(f"Error: {e}")
        print(traceback.print_exc())
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
        print(traceback.print_exc())
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
        print(traceback.print_exc())
        return False
    finally:
        if conn:
            conn.close()
        if cursor:
            cursor.close()


async def get_stats_from_pg_past_year_p1(catalog_list, year, conn_pg):
    results = []
    try:
        params = [(year, catalog[1]) for catalog in catalog_list] 

        query = f"""
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
                                            AND EXTRACT(YEAR FROM TIMEZONE('America/Mexico_City', pol.FECHA)) = $1
                                            AND m.CANCELADO = false
                                            AND prod.CODIGO IN ($2)
                                        GROUP BY
                                            li.id,
                                            sl.id,
                                            li.NOMBRE,
                                            sl.NOMBRE,
                                            EXTRACT(YEAR FROM TIMEZONE('America/Mexico_City', pol.FECHA))
                                      ) sub ;"""
        results = await conn_pg.fetchmany(query, params)
    except Exception as err:
        print(err)
        print(traceback.print_exc())
    finally:
        return results


async def get_existent_abc_records_p1(catalog_list, year, enterprise, conn, query):
    cursor = None
    results = []
    try:

        query = query.format(', '.join([str(catalog[0]) for catalog in catalog_list]))
        params = (year, enterprise)

        cursor = await conn.cursor()
        await cursor.execute(query, params)

        results = await cursor.fetchall()

    except Exception as err:
        print(err)
        print(traceback.print_exc())

    finally:
        if cursor:
            await cursor.close()

        return results



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
    cursor = conn = cursor_pg = conn_pg = None
    try:
        current_year = datetime.datetime.now().year
        conn = await aiom.connect(
            host=ENV_MYSQL_HOST or "localhost",
            user=ENV_MYSQL_USER or "root",
            password=ENV_MYSQL_PASSWORD or "root",
            db=ENV_MYSQL_NAME or "db",
            port=int(ENV_MYSQL_PORT or "3306")
        )
        conn_pg =  await aiop.connect(
                database=ENV_PSQL_NAME,
                user=ENV_PSQL_USER,
                host=ENV_PSQL_HOST,
                password=ENV_PSQL_PASSWORD,
                port=ENV_PSQL_PORT
        )

        cursor = await conn.cursor()
        row_year = None
        await cursor.execute("SELECT year FROM product_abc WHERE year = %s", (year,))        
        row_year_sql = await cursor.fetchone()        
        if row_year_sql is not None:
            row_year = row_year_sql[0]        

        mysql_query = None

        if row_year is None and year < current_year or (int(year) + 1 == current_year):
            mysql_query = """SELECT id 
                        FROM product_abc
                        WHERE catalog_id IN ({0})
                            AND year = %s
                            AND assigned_company = %s
                            AND (total_amount IS NULL OR profit IS NULL OR units_sold IS NULL)                                   
                    """
        elif year == current_year:
            mysql_query = """
                        SELECT id 
                        FROM product_abc 
                        WHERE catalog_id IN ({0})                                         
                           AND year = %s
                           AND assigned_company = %s                                   
                      """

        if not mysql_query:
            print(f'Year {year} doesnt satisfy any conditions')
            return False


        update_query = """UPDATE product_abc SET total_amount = %s, profit = %s, units_sold = %s 
                                            WHERE catalog_id = %s AND year = %s AND assigned_company = %s"""
        async def job(catalog_list):
            stats_list = await get_stats_from_pg_past_year_p1(catalog_list, year, conn_pg)
            existent_abc_records = await get_existent_abc_records_p1(catalog_list, year, enterprise, conn, mysql_query)
            
            if not len(existent_abc_records):
                return 

            update_batch = []
            for index, stats in enumerate(stats_list):
                if stats is None:
                    continue

                abc_record = existent_abc_records[index]
                catalog = catalog_list[index]

                total_amount = stats.get('total_importe')
                profit = stats.get('total_utilidad')
                units_sold = stats.get('unidades_vendidas')
        
                if not len(abc_record):
                    continue

                update_batch.append((total_amount, profit, units_sold, catalog[0], year, enterprise))

            cursor = await conn.cursor()
            await cursor.executemany(update_query, update_batch)


        batch_size = 500
        full_batches = len(catalog_list) // batch_size
        remainder_batch = len(catalog_list) %  batch_size

        for i in range(full_batches):
            start = i * batch_size
            end = (i + 1) * batch_size
            await job(catalog_list[start:end])
            
        if remainder_batch:
            start = full_batches * batch_size
            await job(catalog_list[start:])
        

        return True
        
    except m.Error as e:
        print(f"Error MySQL: {e}")
        return False    
    except Exception as e:
        print(traceback.print_exc())
        print(f"Error: {e}")
        print(traceback.print_exc())
        return False
    finally:
        if cursor is not None:
            await cursor.close()
        if cursor_pg is not None:
            cursor_pg.close()
        if conn is not None:
            conn.close()
        if conn_pg is not None:
            await conn_pg.close()


async def upsert_product_abc_part2(year, enterprise = "marw"):
    conn = cursor = conn_pg = None
    try:
        actual_year = datetime.datetime.now().year
        conn_pg =  await aiop.connect(
                database=ENV_PSQL_NAME,
                user=ENV_PSQL_USER,
                host=ENV_PSQL_HOST,
                password=ENV_PSQL_PASSWORD,
                port=ENV_PSQL_PORT
        )
        conn = await aiom.connect(
            host=ENV_MYSQL_HOST or "localhost",
            user=ENV_MYSQL_USER or "root",
            password=ENV_MYSQL_PASSWORD or "root",
            db=ENV_MYSQL_NAME or "db",
            port=int(ENV_MYSQL_PORT or "3306")
        )
        cursor = await conn.cursor()
        last_month_update = 0
        await cursor.execute("SELECT MAX(last_update) FROM product_abc WHERE year = %s AND assigned_company = %s", (year, enterprise))
        row = await cursor.fetchone()
        if row is not None and row[0] is not None:
            last_month_update = row[0].month

        await cursor.execute("SET SESSION group_concat_max_len = 1000000;")
        if int(year) < actual_year or (int(year) +1 == actual_year and last_month_update < 6):
            await cursor.execute("""
                SELECT abc.id,abc.catalog_id, GROUP_CONCAT(p.code SEPARATOR ', ')
                    FROM 
                        product_abc AS abc INNER JOIN product_catalog AS pc
                            ON abc.catalog_id = pc.catalog_id
                        INNER JOIN product AS p
                            ON pc.product_id = p.id
                    WHERE 
                        abc.year = %s AND 
                        (abc.inventory_close_u IS NULL OR abc.inventory_close_p IS NULL) AND
                        abc.assigned_company = %s AND
                        pc.add_year = %s AND
                        pc.add_year = (
                            SELECT MAX(add_year) 
                            FROM product_catalog 
                            WHERE 
                                catalog_id = abc.catalog_id AND
                                add_year <= %s 
                        )
                    GROUP BY abc.catalog_id
                    ORDER BY abc.id;
            """, (year, enterprise, year, year))

            batch_size = 500
            all_catalogs = await cursor.fetchall()
            batches = len(all_catalogs) // batch_size
            remainder = len(all_catalogs) % batch_size

            async def job(batch):
                updates_params = []
                update_query = "UPDATE product_abc SET inventory_close_u = %s, inventory_close_p = %s WHERE id = %s"
                params = [(year, year, group[2]) for group in batch]
                results = []
                try:
                    results = await conn_pg.fetchmany("""
                                        SELECT 
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
                                                    EXTRACT(YEAR FROM TIMEZONE('America/Mexico_City', p_inv_lookup.FECHA)) < $1 OR
                                                    (EXTRACT(YEAR FROM TIMEZONE('America/Mexico_City', p_inv_lookup.FECHA)) = $2 AND EXTRACT(MONTH FROM TIMEZONE('America/Mexico_City', p_inv_lookup.FECHA)) <= 12)
                                                )
                                                AND prod_inv.CODIGO IN ($3)
                                                GROUP BY
                                                m_inv.almacen_id,
                                                md_inv.producto_id
                                        )  sub """, params)
                except:
                    print(f'\tError on postgres')


                for index, result in enumerate(results):
                    existencias = result.get('existencias')
                    existencias_p = result.get('existencias_p')

                    if not existencias and not existencias_p:
                        continue

                    updates_params.append((existencias, existencias_p, batch[index][0]))

                if len(updates_params) != 0:
                    try:
                        await cursor.executemany(update_query, updates_params)
                    except:
                        print('\tError on updating mysql')

    
            for batch in range(batches):
                catalog_batch = all_catalogs[(batch * batch_size): ((batch + 1) * batch_size)]
                await job(catalog_batch)

            if remainder:
                await job(all_catalogs[batches*batch_size:])
                

            await conn.commit()
            await cursor.close()
            conn.close()
            return True
        elif year == actual_year:
 
            await cursor.execute("""
                SELECT abc.id,abc.catalog_id, GROUP_CONCAT(p.code SEPARATOR ', ')
                    FROM 
                        product_abc AS abc INNER JOIN product_catalog AS pc
                            ON abc.catalog_id = pc.catalog_id
                        INNER JOIN product AS p
                            ON pc.product_id = p.id
                    WHERE 
                        abc.year = %s AND 
                        abc.assigned_company = %s AND
                        pc.add_year = %s AND
                        pc.add_year = (
                            SELECT MAX(add_year) 
                            FROM product_catalog 
                            WHERE 
                                catalog_id = abc.catalog_id AND
                                add_year <= %s 
                        )
                    GROUP BY abc.catalog_id
                    ORDER BY abc.id;
            """, (year, enterprise, year, year))

            batch_size = 500
            all_catalogs = await cursor.fetchall()
            batches = len(all_catalogs) // batch_size
            remainder = len(all_catalogs) % batch_size

            async def job(batch):
                updates_params = []
                update_query = """
                    INSERT INTO product_abc 
                        (catalog_id, inventory_close_u, inventory_close_p, year, enterprise, last_update) 
                        VALUES (%s, %s, %s, %s, %s, NOW()) 
                    ON DUPLICATE UPDATE
                            inventory_close_u = VALUES(inventory_close_u),
                            inventory_close_p = VALUES(inventory_close_p),
                            last_update = VALUES(last_update)
                    """
                params = [(year, year, group[2]) for group in batch]
                results = []
                try:
                    results = await conn_pg.fetchmany("""
                                        SELECT
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
                                        sub.rn = 1;""", params)
                except:
                    print(f'\tError on postgres')


                for index, result in enumerate(results):
                    inventario_cierre_u = result.get('inventario_cierre_u')
                    inventario_cierre_p = result.get('inventario_cierre_p')

                    if not inventario_cierre_u and not inventario_cierre_p:
                        continue

                    updates_params.append((batch[index][0], inventario_cierre_u, inventario_cierre_p, year, enterprise))

                if len(updates_params) != 0:
                    try:
                        await cursor.executemany(update_query, updates_params)
                    except:
                        print('\tError on updating mysql')

    
            for batch in range(batches):
                catalog_batch = all_catalogs[(batch * batch_size): ((batch + 1) * batch_size)]
                await job(catalog_batch)

            if remainder:
                await job(all_catalogs[batches*batch_size:])
                

            await conn.commit()
            await cursor.close()
            conn.close()

            return True
        else:
            pass
            await conn.commit()
            await cursor.close()
            conn.close()
            return True    
    except Exception as e:        
        print(f"Error: {e}")
        print(traceback.print_exc())
        return False
    finally:
        if cursor:
            await cursor.close()        
        if conn:
            conn.close()        
        if conn_pg:
            await conn_pg.close()


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

        if year < actual_year or (year+1 == actual_year and last_month_update < 6):
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
        print(traceback.print_exc())
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

        if year < actual_year or (year+1 == actual_year and last_month_update < 6):
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
        print(traceback.print_exc())
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

        if year < actual_year or (year+1 == actual_year and last_month_update < 6):
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
        print(traceback.print_exc())
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

        if year < actual_year or (year+1 == actual_year and last_month_update < 6):
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
        print(traceback.print_exc())
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

        if year < actual_year or (year+1 == actual_year and last_month_update < 6):
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
        print(traceback.print_exc())
        return False
    finally:
        if conn:
            conn.close()
        if cursor:
            cursor.close()

async def upsert_all(year, enterprise):
    try:
        # print("#####################################")
        # print("PART 0")
        await upsert_product_abc_part0(year, enterprise)
        catalog_list = await get_product_catalogs(year)        
        if catalog_list:               
            # print("#####################################")
            # print("PART 1")
            await upsert_product_abc_part1(catalog_list, year, enterprise)        
            # print("#####################################")
            # print("PART 2")
            await upsert_product_abc_part2(year, enterprise)
        #         await upsert_product_abc_part3(year)
        #         await upsert_product_abc_part4(year)
        #         await upsert_product_abc_part5(year)
        #         await upsert_product_abc_part6(year)
        #         await upsert_product_abc_part7(year)
        #         return True
        # else:
        #     return False
    except Exception as e:
        print(f"Error: {e}")
        print(traceback.print_exc())
        return False
