from apps.home.queries.mysql import UPSERT_CATALOGS, UPSERT_FAMILIES, UPSERT_MOVEMENT_DETAILS, UPSERT_MOVEMENTS, UPSERT_PRODUCTS, UPSERT_SUBFAMILIES, UPSERT_BRANDS
from apps.home.queries.postgres import SELECT_BRANDS, SELECT_FAMILIES, SELECT_MOVEMENT_DETAILS, SELECT_MOVEMENTS, SELECT_PRODUCTS, SELECT_SUBFAMILIES
from core.settings import ENV_PSQL_NAME, ENV_PSQL_USER, ENV_PSQL_PASSWORD, ENV_PSQL_HOST, ENV_PSQL_PORT, ENV_PSQL_DB_SCHEMA, ENV_MYSQL_HOST, ENV_MYSQL_PORT, ENV_MYSQL_NAME, ENV_MYSQL_USER, ENV_MYSQL_PASSWORD, ENV_UPDATE_ALL_DATES
import asyncpg
import aiomysql
from dataclasses import dataclass
import pandas as pd
import asyncio
import warnings


warnings.filterwarnings('ignore', module=r"aiomysql")

@dataclass
class EnterpriseConnectionData:
    schema: str
    name: str
    user: str
    password: str
    host: str
    port: int



enterprises = {
    "MR DIESEL" : EnterpriseConnectionData(
        schema=str(ENV_PSQL_DB_SCHEMA),
        port=int(ENV_PSQL_PORT or 5432),
        user=str(ENV_PSQL_USER),
        password=str(ENV_PSQL_PASSWORD),
        host=str(ENV_PSQL_HOST),
        name=str(ENV_PSQL_NAME)
    )
}


async def upsert_catalog(my_pool: aiomysql.Pool, catalog_path: str, catalog_name_column: str) -> int:
    """Reads the catalog file and inserts the catalog names in the application database

    Returns
    -------
    int
        Number of affected rows on the application database. If there was no return on postgres or mysql it returns -1.
    """
    catalog_df = pd.read_excel(catalog_path)
    catalog_names = catalog_df[catalog_name_column].unique() 


    my_query = UPSERT_CATALOGS()
    query_params = [(catalog) for catalog in catalog_names]

    affected = -1
    async with my_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.executemany(my_query, query_params)
            affected = cursor.rowcount

        await conn.commit()

    return affected
    
async def upsert_families(pg_pool: asyncpg.Pool, my_pool: aiomysql.Pool, schema: str) -> int:
    """Fetch families for a given schema in admintotal and upserts it in the application database

    Returns
    -------
    int
        Number of affected rows on the application database. If there was no return on postgres or mysql it returns -1.
    """
    pg_query = SELECT_FAMILIES(schema)

    families = []
    async with pg_pool.acquire() as conn:
        families = await conn.fetch(pg_query)   

    if len(families) == 0:
        return -1 

    query_params = [(family.get('nombre'), family.get('id')) for family in families]

    my_query = UPSERT_FAMILIES(schema)

    affected = -1
    async with my_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.executemany(my_query, query_params)
            affected = cursor.rowcount
        await conn.commit()

    return affected



async def upsert_subfamilies(pg_pool: asyncpg.Pool, my_pool: aiomysql.Pool, schema: str) -> int:
    """Fetch subfamilies for a given schema in admintotal and upserts it in the application database

    Returns
    -------
    int
        Number of affected rows on the application database. If there was no return on postgres or mysql it returns -1.
    """
    pg_query = SELECT_SUBFAMILIES(schema)

    subfamilies = []
    async with pg_pool.acquire() as conn:
        subfamilies = await conn.fetch(pg_query)   

    if len(subfamilies) == 0:
        return -1 

    query_params = [(subfamily.get('nombre'), subfamily.get('id')) for subfamily in subfamilies]

    my_query = UPSERT_SUBFAMILIES(schema)

    affected = -1
    async with my_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.executemany(my_query, query_params)
            affected = cursor.rowcount
        await conn.commit()

    return affected

async def upsert_brands(pg_pool: asyncpg.Pool, my_pool: aiomysql.Pool, schema:str) -> int:
    """Fetch brands for a given schema in admintotal and upserts it in the application database

    Returns
    -------
    int
        Number of affected rows on the application database. If there was no return on postgres or mysql it returns -1.
    """
    pg_query = SELECT_BRANDS(schema)

    brands = []
    async with pg_pool.acquire() as conn:
        brands = await conn.fetch(pg_query)   

    if len(brands) == 0:
        return -1 

    query_params = [(brand.get('nombre'), brand.get('id')) for brand in brands]

    my_query = UPSERT_BRANDS(schema)

    affected = -1
    async with my_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.executemany(my_query, query_params)
            affected = cursor.rowcount
        await conn.commit()

    return affected

async def upsert_products(pg_pool: asyncpg.Pool, my_pool: aiomysql.Pool, schema: str) -> int:
    """Upserts products on the application database after requesting admintotal's postgres database.
    This function request products in chunks of 200MB and each of them is inserted in chunks of 4MB.

    Return
    ------
    int
        Number of affected rows in the application database
    """

    #The limit for us is 200MB but we have to transform it to bytes for further operations
    MAX_QUERY_WEIGHT_IN_RAM=200 * (1024 ** 2)

    #For estimating the row size for each returned field in the query we have to look at the sizes given by python
    #28 bytes on ints, 48 on datetime objects, 141 on a string of 100 char and 521 on a string of 512 and an overhead
    #of 100 bytes in Record instances

    #There are 4 ints in the query and we will add an extra 10% of safety
    ESTIMATED_ROW_SIZE = ((28 * 4) + 48 + 141 + 521 + 100) * 1.1

    ESTIMATED_LIMIT_OF_ROWS = int(MAX_QUERY_WEIGHT_IN_RAM // ESTIMATED_ROW_SIZE) 


    MAX_MYSQL_QUERIES_PER_REQUEST = 60_000


    pg_current_page = 0
    pg_results = []

    my_affected_rows = 0
    my_query = UPSERT_PRODUCTS(schema)

    while True:
        async with pg_pool.acquire() as pg_conn:
            pg_query = SELECT_PRODUCTS(
                schema,
                limit=ESTIMATED_LIMIT_OF_ROWS,
                offset=(ESTIMATED_LIMIT_OF_ROWS * pg_current_page)
            )

            pg_results = await pg_conn.fetch(pg_query)

        if len(pg_results) == 0:
            break

        query_params =  [tuple([*result.values()]) for result in pg_results]
        
        my_complete_pages = len(pg_results) // MAX_MYSQL_QUERIES_PER_REQUEST
        my_reminder_page = len(pg_results) % MAX_MYSQL_QUERIES_PER_REQUEST


        async with my_pool.acquire() as my_conn:
            async with my_conn.cursor() as cursor:
                for pagenum in range (my_complete_pages):
                    page = query_params[(pagenum * MAX_MYSQL_QUERIES_PER_REQUEST):((pagenum + 1) * MAX_MYSQL_QUERIES_PER_REQUEST)]

                    await cursor.executemany(my_query, page)
                    my_affected_rows += cursor.rowcount

                if my_reminder_page:
                    await cursor.executemany(
                        my_query,
                        query_params[(my_complete_pages * MAX_MYSQL_QUERIES_PER_REQUEST):]
                    )
                    my_affected_rows += cursor.rowcount

            await my_conn.commit()
        
        if len(pg_results) < ESTIMATED_LIMIT_OF_ROWS:
            break

        pg_current_page += 1

    return my_affected_rows


async def upsert_movements(pg_pool: asyncpg.Pool, my_pool: aiomysql.Pool, schema: str) -> int:
    """Upserts movements on the application database after requesting admintotal's postgres database.
    This function request products in chunks of 200MB and each of them is inserted in chunks of 4MB.

    Return
    ------
    int
        Number of affected rows in the application database
    """
    #The limit for us is 200MB but we have to transform it to bytes for further operations
    MAX_QUERY_WEIGHT_IN_RAM=200 * (1024 ** 2)

    #For estimating the row size for each returned field in the query we have to look at the sizes given by python
    #28 bytes on ints, 48 on datetime objects, 3 strings with 49 as overhead and lengths: (255 + 255 + 20 = 530),
    #6 booleans, 9 numeric values with lenght of 100 and an overhead of 248 bytes in Record instances

    #For further research use the pympler package

    #(5 ints) + (1 datetime) + (3 strings of 255, 255 and 20) + (6 booleans) + (9 numerics) + Record overhead
    ESTIMATED_ROW_SIZE = ((28 * 5) + 48 + ((3 * 49)+ 530) + (28 * 6)+ (100 * 9) + 248) * 1.1

    ESTIMATED_LIMIT_OF_ROWS = int(MAX_QUERY_WEIGHT_IN_RAM // ESTIMATED_ROW_SIZE) 


    MAX_MYSQL_QUERIES_PER_REQUEST = 60_000

    pg_current_page = 0
    pg_results = []

    my_affected_rows = 0
    my_query = UPSERT_MOVEMENTS(schema)

    while True:
        async with pg_pool.acquire() as pg_conn:
            pg_query = SELECT_MOVEMENTS(
                schema,
                limit=ESTIMATED_LIMIT_OF_ROWS,
                offset=(ESTIMATED_LIMIT_OF_ROWS * pg_current_page)
            )

            pg_results = await pg_conn.fetch(pg_query)

        if len(pg_results) == 0:
            break

        query_params =  [tuple([*result.values()]) for result in pg_results]
        
        my_complete_pages = len(pg_results) // MAX_MYSQL_QUERIES_PER_REQUEST
        my_reminder_page = len(pg_results) % MAX_MYSQL_QUERIES_PER_REQUEST


        async with my_pool.acquire() as my_conn:
            async with my_conn.cursor() as cursor:
                for pagenum in range (my_complete_pages):
                    page = query_params[(pagenum * MAX_MYSQL_QUERIES_PER_REQUEST):((pagenum + 1) * MAX_MYSQL_QUERIES_PER_REQUEST)]

                    await cursor.executemany(my_query, page)
                    my_affected_rows += cursor.rowcount

                if my_reminder_page:
                    await cursor.executemany(
                        my_query,
                        query_params[(my_complete_pages * MAX_MYSQL_QUERIES_PER_REQUEST):]
                    )
                    my_affected_rows += cursor.rowcount

            await my_conn.commit()
        
        if len(pg_results) < ESTIMATED_LIMIT_OF_ROWS:
            break

        pg_current_page += 1

    return my_affected_rows

async def upsert_movement_details(pg_pool: asyncpg.Pool, my_pool: aiomysql.Pool, schema: str) -> int:
    """Upserts movement details on the application database after requesting admintotal's postgres database.
    This function request products in chunks of 200MB and each of them is inserted in chunks of 4MB.

    Return
    ------
    int
        Number of affected rows in the application database
    """
     #The limit for us is 200MB but we have to transform it to bytes for further operations
    MAX_QUERY_WEIGHT_IN_RAM=200 * (1024 ** 2)

    #For estimating the row size for each returned field in the query we have to look at the sizes given by python
    #28 bytes on ints, 48 on datetime objects, 1 string with 49 as overhead and length 100,
    #1 boolean, 9 numeric values with lenght of 100 and an overhead of 248 bytes in Record instances

    #For further research use the pympler package

    #(3 ints) + (1 datetime) + (1 string of 100 char) + (1 boolean) + (9 numerics) + Record overhead
    ESTIMATED_ROW_SIZE = ((28 * 5) + 48 + (149) + (28)+ (100 * 9) + 248) * 1.1

    ESTIMATED_LIMIT_OF_ROWS = int(MAX_QUERY_WEIGHT_IN_RAM // ESTIMATED_ROW_SIZE) 


    MAX_MYSQL_QUERIES_PER_REQUEST = 200_000

    pg_current_page = 0
    pg_results = []

    my_affected_rows = 0
    my_query = UPSERT_MOVEMENT_DETAILS(schema)

    while True:
        async with pg_pool.acquire() as pg_conn:
            pg_query = SELECT_MOVEMENT_DETAILS(
                schema,
                limit=ESTIMATED_LIMIT_OF_ROWS,
                offset=(ESTIMATED_LIMIT_OF_ROWS * pg_current_page)
            )

            pg_results = await pg_conn.fetch(pg_query)

        if len(pg_results) == 0:
            break

        query_params =  [tuple([*result.values()]) for result in pg_results]
        
        my_complete_pages = len(pg_results) // MAX_MYSQL_QUERIES_PER_REQUEST
        my_reminder_page = len(pg_results) % MAX_MYSQL_QUERIES_PER_REQUEST


        async with my_pool.acquire() as my_conn:
            async with my_conn.cursor() as cursor:
                for pagenum in range (my_complete_pages):
                    page = query_params[(pagenum * MAX_MYSQL_QUERIES_PER_REQUEST):((pagenum + 1) * MAX_MYSQL_QUERIES_PER_REQUEST)]

                    await cursor.executemany(my_query, page)
                    my_affected_rows += cursor.rowcount

                if my_reminder_page:
                    await cursor.executemany(
                        my_query,
                        query_params[(my_complete_pages * MAX_MYSQL_QUERIES_PER_REQUEST):]
                    )
                    my_affected_rows += cursor.rowcount

            await my_conn.commit()
        
        if len(pg_results) < ESTIMATED_LIMIT_OF_ROWS:
            break

        pg_current_page += 1

    return my_affected_rows



   


async def refresh_data(enterprise: str, catalog_path: str, catalog_name_column: str):
    """Fetch families, subfamilies and brands for the schema of a given enterprise in admintotal's postgres and
    inserts them in their respective tables in the application's database
    """
    connection_data = enterprises.get(enterprise)

    if not connection_data:
        return None

    pg_pool = await asyncpg.create_pool(
        user=connection_data.user,
        host=connection_data.host,
        port=connection_data.port,
        database=connection_data.name,
        password=connection_data.password,
        min_size=3,
        max_size=5,
        max_inactive_connection_lifetime=300
    )

    my_pool = await aiomysql.create_pool(
        user=ENV_MYSQL_USER,
        host=ENV_MYSQL_HOST,
        port=int(ENV_MYSQL_PORT or 3306),
        password=ENV_MYSQL_PASSWORD,
        db=ENV_MYSQL_NAME,
        minsize=3,
        maxsize=5
    )

    todos = [upsert_families, upsert_subfamilies, upsert_brands]
    tasks = []
    async with asyncio.TaskGroup() as tg:
        for todo in todos:
            task = tg.create_task(
                todo(pg_pool=pg_pool, my_pool=my_pool, schema=connection_data.schema)
            )
            tasks.append(task)

        tasks.append(
            tg.create_task(
                upsert_catalog(my_pool=my_pool, catalog_path=catalog_path, catalog_name_column=catalog_name_column)
            )
        )

    affected_products = await upsert_products(pg_pool=pg_pool, my_pool=my_pool, schema=connection_data.schema)

    affected_movements = await upsert_movements(pg_pool=pg_pool, my_pool=my_pool, schema=connection_data.schema)
 
    affected_movement_details = await upsert_movement_details(pg_pool=pg_pool, my_pool=my_pool, schema=connection_data.schema)

    for task in tasks:
        print(f'Result on task {task.get_name()}: {task.result()}')

    print(f'Products affected: {affected_products}')
    print(f'Movements affected: {affected_movements}')
    print(f'Movement details affected: {affected_movement_details}')




 
        

        

### Enterprises funciona para ir agregando mas empresas y sus respectivos datos de conexion a la base de datos, tomarlo en cuenta al traer los datos
### Existe la variable ENV_UPDATE_ALL_DATES que si es True no se toma en cuenta el ultimo año o mes o fecha de actualización y se actualizan todos los registros

###### La forma de trabajo y el orden de inserción principal es el siguiente: 
# Traer familias:
### SELECT nombre, id FROM {schema de la empresa}.admintotal_linea

# Traer subfamilias:
### SELECT nombre, id FROM {schema de la empresa}.admintotal_sublinea

# Traer marcas (pueden ser null porque no todos los productos tienen marca):
### SELECT nombre, id FROM {schema de la empresa}.admintotal_productomarca

# Traer productos:
### SELECT p.sublinea_id, p.linea_id, p.marca_id, p.id, p.codigo, p.descripcion, p.creado FROM {schema de la empresa}.admintotal_producto as p

# Insertar catalogos (pueden ser null porque se insertan del excel):
### INSERT INTO catalog (name) VALUES (%s) 

# Insertar movimientos (los puse en orden según yo):
# SELECT m.poliza_ptr_id, p.fecha,
# 	   m.es_orden, m.es_entrada, m.es_salida, m.pendiente, 
#      m.folio, m.serie,
# 	   m.folio_adicional, m.nota_pagada,   
# 	   CASE
#        		WHEN m.metodo_pago IS NULL OR m.metodo_pago = '' OR m.metodo_pago = '99' THEN '99 Por definir'
#             WHEN m.metodo_pago = '01' THEN '01 Efectivo'
#             WHEN m.metodo_pago = '02' THEN '02 Cheque nominativo'
#             WHEN m.metodo_pago = '03' THEN '03 Transferencia electrónica de fondos'
#             WHEN m.metodo_pago = '04' THEN '04 Tarjeta de crédito'
#             WHEN m.metodo_pago = '05' THEN '05 Monedero electrónico'
#             WHEN m.metodo_pago = '06' THEN '06 Dinero electrónico'
#             WHEN m.metodo_pago = '08' THEN '08 Vales de despensa'
#             WHEN m.metodo_pago = '12' THEN '12 Dación en pago'
#             WHEN m.metodo_pago = '13' THEN '13 Pago por subrogación'
#             WHEN m.metodo_pago = '14' THEN '14 Pago por consignación'
#             WHEN m.metodo_pago = '15' THEN '15 Condonación'
#             WHEN m.metodo_pago = '17' THEN '17 Compensación'
#             WHEN m.metodo_pago = '23' THEN '23 Novación'
#             WHEN m.metodo_pago = '24' THEN '24 Confusión'
#             WHEN m.metodo_pago = '25' THEN '25 Remisión de deuda'
#             WHEN m.metodo_pago = '26' THEN '26 Prescripción o caducidad'
#             WHEN m.metodo_pago = '27' THEN '27 A satisfacción del acreedor'
#             WHEN m.metodo_pago = '28' THEN '28 Tarjeta de débito'
#             WHEN m.metodo_pago = '29' THEN '29 Tarjeta de servicio'
#             WHEN m.metodo_pago = '30' THEN '30 Aplicación de anticipos'
#             WHEN m.metodo_pago = '31' THEN '31 Intermediario pagos'
#           END AS metodo_pago,
#       ROUND(SUM(md.cantidad), 5) AS cantidad,
#       CASE
#           WHEN ROUND(SUM(md.cantidad*md.factor_um), 5) IS NULL THEN 0 
#           ELSE ROUND(SUM(md.cantidad*md.factor_um), 5)
#       END AS cantidad_total,
# 	   ROUND(m.importe, 5) AS importe, 
#        ROUND(m.iva, 5) AS iva, 
#        ROUND(m.descuento, 5) AS descuento,
# 	   ROUND(m.importe_descuento, 5) AS importe_descuento,
#        ROUND(m.total, 5) AS total,
#        ROUND(m.costo_venta, 5) AS costo_venta, 
#        ROUND((m.importe - m.costo_venta), 5) AS utilidad,
#        mx.nombre AS moneda,
#        m.orden_id as orden_id,
# 	   m.tipo_movimiento,
# 	   m.cancelado
# 	   FROM
# 	   		{schema de la empresa}.admintotal_movimiento m
# 	   INNER JOIN
# 	   		{schema de la empresa}.admintotal_poliza p ON p.id = m.poliza_ptr_id
# 	   LEFT JOIN
# 	   		{schema de la empresa}.admintotal_movimientodetalle md ON md.movimiento_id = m.poliza_ptr_id
# 	   LEFT JOIN
# 	   		{schema de la empresa}.admintotal_moneda mx ON mx.id = m.moneda_id
# 	   GROUP BY
# 	   		m.poliza_ptr_id,
# 			p.fecha,
# 			m.folio,
# 			m.nota_pagada,
# 			m.importe,
# 			m.iva,
# 			m.total,
# 			m.costo_venta,
# 			m.cancelado,
# 			mx.nombre

# Insertar movimientos detalle (insertar todo aunque producto_id sea null):
# SELECT md.id, md.movimiento_id, md.producto_id, md.fecha, um.nombre, 
# 	  md.cantidad, md.factor_um, md.precio_unitario,
# 	  CASE
#          WHEN ROUND(SUM(md.cantidad*md.factor_um), 5) IS NULL THEN 0 
#          ELSE ROUND(SUM(md.cantidad*md.factor_um), 5)
#       END AS cantidad_total,
# 	  md.importe, md.iva, md.descuento, md.existencia, md.costo_venta, md.cancelado
# 	  FROM 
# 	  		prueba9.admintotal_movimientodetalle md
# 	  LEFT JOIN
# 	  		prueba9.admintotal_um um ON um.id = md.um_id
# 	  GROUP BY
# 	  		md.id,
# 			um.nombre


### Avisarme para iniciar lo del análisis ABC y producto ABC si se necesita ayuda
# Insertar en el año ej.2022, 2023, 2024 del producto abc todos los productos que se hayan insertado en ese año para abajo solamente
# El timestap del last_update se actualiza al terminar de insertar todos los datos disponibles en el año de x producto

# Insertar en el año ej.2022, 2023, 2024 del análisis abc todos los catalogos que tengan al menos un producto en ese año para abajo solamente (si un catalogo tiene varias familias y varias subfamilias, se inserta uno diferenta para cada agrupacion de estas)
# El timestap del last_update se actualiza al terminar de insertar todos los datos disponibles en el año de x catalogo
