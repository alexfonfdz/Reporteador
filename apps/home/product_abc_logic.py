from apps.home.queries.mysql import UPDATE_PRODUCT_CATALOG, UPSERT_CATALOGS, UPSERT_FAMILIES, UPSERT_MOVEMENT_DETAILS, UPSERT_MOVEMENTS, UPSERT_PRODUCTS, UPSERT_SUBFAMILIES, UPSERT_BRANDS,GET_DISTINCT_YEARS_MOVEMENTS, GET_PRODUCTS_SALES_SUMMARY_BY_YEAR, GET_TOTAL_AMOUNT_AND_TOTAL_PROFIT_BY_YEAR, UPSERT_PRODUCT_ABC, GET_DISTINCT_YEARS_MOVEMENTS_ALL, GET_TOTAL_AMOUNT_AND_TOTAL_PROFIT_BY_YEAR_ALL, GET_PRODUCTS_SALES_SUMMARY_BY_YEAR_ALL, GET_MOST_RECENT_UPDATE_UTC, INSERT_TABLE_UPDATE, UPSERT_ANALYSIS_ABC
from datetime import timedelta
from apps.home.queries.postgres import SELECT_BRANDS, SELECT_FAMILIES, SELECT_MOVEMENT_DETAILS, SELECT_MOVEMENTS, SELECT_PRODUCTS, SELECT_SUBFAMILIES
from core.settings import ENV_PSQL_NAME, ENV_PSQL_USER, ENV_PSQL_PASSWORD, ENV_PSQL_HOST, ENV_PSQL_PORT, ENV_PSQL_DB_SCHEMA, ENV_MYSQL_HOST, ENV_MYSQL_PORT, ENV_MYSQL_NAME, ENV_MYSQL_USER, ENV_MYSQL_PASSWORD, ENV_UPDATE_ALL_DATES
import asyncpg
import aiomysql
from dataclasses import dataclass
import pandas as pd
import asyncio
import datetime
from collections import defaultdict

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

async def upsert_products(pg_pool: asyncpg.Pool, my_pool: aiomysql.Pool, schema: str, update_all: bool) -> int:
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
    update_table_query = INSERT_TABLE_UPDATE(schema)


    #Now we seek in our database for the last change we did in the table in UTC
    most_recent_change_query = GET_MOST_RECENT_UPDATE_UTC(schema)

    most_recent_change = None
    async with my_pool.acquire() as my_conn:
        async with my_conn.cursor() as cursor:
            await cursor.execute(most_recent_change_query, ('product'))

            result = await cursor.fetchone()

            if result is not None and result[0] is not None:
                most_recent_change = result[0]



    if most_recent_change is None or update_all:
        async def get_results(conn: asyncpg.Connection, query: str):
            return await conn.fetch(query)

        def get_pg_query(offset: int):
            return SELECT_PRODUCTS(
                schema,
                limit=ESTIMATED_LIMIT_OF_ROWS,
                offset=offset,
                with_date=False
            )
    else:
        async def get_results(conn: asyncpg.Connection, query: str):
            return await conn.fetch(query, most_recent_change)

        def get_pg_query(offset: int):
            return SELECT_PRODUCTS(
                schema,
                limit=ESTIMATED_LIMIT_OF_ROWS,
                offset=offset,
                with_date=True
            )


    while True:
        async with pg_pool.acquire() as pg_conn:
            pg_query = get_pg_query(
                offset=(ESTIMATED_LIMIT_OF_ROWS * pg_current_page),
            )

            pg_results = await get_results(pg_conn, pg_query)

        if len(pg_results) == 0:
            break

        query_params =  [tuple([*result.values()]) for result in pg_results]

        my_complete_pages = len(pg_results) // MAX_MYSQL_QUERIES_PER_REQUEST
        my_reminder_page = len(pg_results) % MAX_MYSQL_QUERIES_PER_REQUEST


        async with my_pool.acquire() as my_conn:
            transaction_count = 0
            async with my_conn.cursor() as cursor:
                for pagenum in range (my_complete_pages):
                    page = query_params[(pagenum * MAX_MYSQL_QUERIES_PER_REQUEST):((pagenum + 1) * MAX_MYSQL_QUERIES_PER_REQUEST)]

                    await cursor.executemany(my_query, page)
                    my_affected_rows += cursor.rowcount
                    transaction_count += cursor.rowcount

                if my_reminder_page:
                    await cursor.executemany(
                        my_query,
                        query_params[(my_complete_pages * MAX_MYSQL_QUERIES_PER_REQUEST):]
                    )
                    my_affected_rows += cursor.rowcount
                    transaction_count += cursor.rowcount
                if transaction_count:
                    await cursor.execute(update_table_query,('product', transaction_count))

            await my_conn.commit()
        
        if len(pg_results) < ESTIMATED_LIMIT_OF_ROWS:
            break

        pg_current_page += 1

    return my_affected_rows

async def upsert_product_catalog(
        my_pool: aiomysql.Pool,
        enterprise: str,
        catalog_path: str,
        catalog_name_column: str,
        product_code_column: str
):
    """Reads the catalog file and inserts the catalog ids in the application database for each product

    Returns
    -------
    int
        Number of affected rows on the application database. If there was no return on postgres or mysql it returns -1.
    """
    catalog_df = pd.read_excel(catalog_path)

    aggregate_fun = lambda x: ','.join(x)

    catalog_products_df = catalog_df.groupby(
        catalog_name_column,
        as_index=False
    ).agg({
        product_code_column: aggregate_fun
    })

    query = UPDATE_PRODUCT_CATALOG(enterprise)
    query_params = [(catalog_name, tuple(products.split(','))) 
        for catalog_name, products in catalog_products_df.itertuples(index=False, name=None)]


    affected_rows = -1
    async with my_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.executemany(query, query_params)

            affected_rows = cursor.rowcount

        await conn.commit()


    return affected_rows

async def upsert_movements(pg_pool: asyncpg.Pool, my_pool: aiomysql.Pool, schema: str, update_all: bool) -> int:
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
    update_table_query = INSERT_TABLE_UPDATE(schema)

    #Now we seek in our database for the last change we did in the table in UTC
    most_recent_change_query = GET_MOST_RECENT_UPDATE_UTC(schema)

    most_recent_change = None
    async with my_pool.acquire() as my_conn:
        async with my_conn.cursor() as cursor:
            await cursor.execute(most_recent_change_query, ('movements'))

            result = await cursor.fetchone()

            if result is not None and result[0] is not None:
                most_recent_change = result[0]


    if most_recent_change is None or update_all:
        async def get_results(conn: asyncpg.Connection, query: str):
            return await conn.fetch(query)

        def get_pg_query(offset: int):
            return SELECT_MOVEMENTS(
                schema,
                limit=ESTIMATED_LIMIT_OF_ROWS,
                offset=offset,
                with_date=False
            )
    else:

        valid_days_to_change_a_movement = timedelta(days=5)
        most_recent_change -= valid_days_to_change_a_movement

        async def get_results(conn: asyncpg.Connection, query: str):
            return await conn.fetch(query, most_recent_change)

        def get_pg_query(offset: int):
            return SELECT_MOVEMENTS(
                schema,
                limit=ESTIMATED_LIMIT_OF_ROWS,
                offset=offset,
                with_date=True
            )


    while True:
        async with pg_pool.acquire() as pg_conn:
            pg_query = get_pg_query(
                offset=(ESTIMATED_LIMIT_OF_ROWS * pg_current_page),
            )

            pg_results = await get_results(pg_conn, pg_query)

        if len(pg_results) == 0:
            break

        query_params =  [tuple([*result.values()]) for result in pg_results]
        
        my_complete_pages = len(pg_results) // MAX_MYSQL_QUERIES_PER_REQUEST
        my_reminder_page = len(pg_results) % MAX_MYSQL_QUERIES_PER_REQUEST


        async with my_pool.acquire() as my_conn:
            transaction_count = 0
            async with my_conn.cursor() as cursor:
                for pagenum in range (my_complete_pages):
                    page = query_params[(pagenum * MAX_MYSQL_QUERIES_PER_REQUEST):((pagenum + 1) * MAX_MYSQL_QUERIES_PER_REQUEST)]

                    await cursor.executemany(my_query, page)
                    my_affected_rows += cursor.rowcount
                    transaction_count += cursor.rowcount

                if my_reminder_page:
                    await cursor.executemany(
                        my_query,
                        query_params[(my_complete_pages * MAX_MYSQL_QUERIES_PER_REQUEST):]
                    )
                    my_affected_rows += cursor.rowcount
                    transaction_count += cursor.rowcount

                if transaction_count:    
                    await cursor.execute(update_table_query, ('movements', transaction_count))

            await my_conn.commit()

        
        if len(pg_results) < ESTIMATED_LIMIT_OF_ROWS:
            break

        pg_current_page += 1

    return my_affected_rows

async def upsert_movement_details(pg_pool: asyncpg.Pool, my_pool: aiomysql.Pool, schema: str, update_all: bool) -> int:
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
    update_table_query = INSERT_TABLE_UPDATE(schema)

    #Now we seek in our database for the last change we did in the table in UTC
    most_recent_change_query = GET_MOST_RECENT_UPDATE_UTC(schema)

    most_recent_change = None
    async with my_pool.acquire() as my_conn:
        async with my_conn.cursor() as cursor:
            await cursor.execute(most_recent_change_query, ('movements_detail'))

            result = await cursor.fetchone()

            if result is not None and result[0] is not None:
                most_recent_change = result[0]


    if most_recent_change is None or update_all:
        async def get_results(conn: asyncpg.Connection, query: str):
            return await conn.fetch(query)

        def get_pg_query(offset: int):
            return SELECT_MOVEMENT_DETAILS(
                schema,
                limit=ESTIMATED_LIMIT_OF_ROWS,
                offset=offset,
                with_date=False
            )
    else:

        valid_days_to_change_a_movement = timedelta(days=5)
        most_recent_change -= valid_days_to_change_a_movement

        async def get_results(conn: asyncpg.Connection, query: str):
            return await conn.fetch(query, most_recent_change)

        def get_pg_query(offset: int):
            return SELECT_MOVEMENT_DETAILS(
                schema,
                limit=ESTIMATED_LIMIT_OF_ROWS,
                offset=offset,
                with_date=True
            )



    while True:
        async with pg_pool.acquire() as pg_conn:
            pg_query = get_pg_query(
                offset=(ESTIMATED_LIMIT_OF_ROWS * pg_current_page),
            )

            pg_results = await get_results(pg_conn, pg_query)

        if len(pg_results) == 0:
            break

        query_params =  [tuple([*result.values()]) for result in pg_results]
        
        my_complete_pages = len(pg_results) // MAX_MYSQL_QUERIES_PER_REQUEST
        my_reminder_page = len(pg_results) % MAX_MYSQL_QUERIES_PER_REQUEST


        async with my_pool.acquire() as my_conn:
            transaction_count = 0
            async with my_conn.cursor() as cursor:
                for pagenum in range (my_complete_pages):
                    page = query_params[(pagenum * MAX_MYSQL_QUERIES_PER_REQUEST):((pagenum + 1) * MAX_MYSQL_QUERIES_PER_REQUEST)]

                    await cursor.executemany(my_query, page)
                    my_affected_rows += cursor.rowcount
                    transaction_count += cursor.rowcount

                if my_reminder_page:
                    await cursor.executemany(
                        my_query,
                        query_params[(my_complete_pages * MAX_MYSQL_QUERIES_PER_REQUEST):]
                    )
                    my_affected_rows += cursor.rowcount
                    transaction_count += cursor.rowcount

                if transaction_count:
                    await cursor.execute(update_table_query, ('movements_detail', transaction_count))

            await my_conn.commit()
        
        if len(pg_results) < ESTIMATED_LIMIT_OF_ROWS:
            break

        pg_current_page += 1

    return my_affected_rows



   


async def refresh_data(enterprise: str, catalog_path: str, catalog_name_column: str, product_code_column: str):
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

    affected_products = await upsert_products(
        pg_pool=pg_pool,
        my_pool=my_pool,
        schema=connection_data.schema,
        update_all=False
    )

    affected_product_catalog = await upsert_product_catalog(
        my_pool=my_pool,
        enterprise=connection_data.schema,
        catalog_path=catalog_path,
        catalog_name_column=catalog_name_column,
        product_code_column=product_code_column
    )

    affected_movements = await upsert_movements(
        pg_pool=pg_pool,
        my_pool=my_pool,
        schema=connection_data.schema,
        update_all=False
    )

    affected_movement_details = await upsert_movement_details(
        pg_pool=pg_pool,
        my_pool=my_pool,
        schema=connection_data.schema,
        update_all=False
    )

    for task in tasks:
        print(f'Result on task {task.get_name()}: {task.result()}')

    print(f'Products affected: {affected_products}')
    print(f'Affected product catalog_rows: {affected_product_catalog}')
    print(f'Movements affected: {affected_movements}')
    print(f'Movements details affected: {affected_movement_details}')


def get_schema_from_enterprise(enterprise_or_schema: str):
    # Si es una empresa registrada, devuelve el schema; si no, asume que ya es un schema o 'TODO'
    if enterprise_or_schema in enterprises:
        return enterprises[enterprise_or_schema].schema
    return enterprise_or_schema

### Product ABC 
async def calculate_product_abc(my_pool, enterprise_or_schema: str):
    schema = get_schema_from_enterprise(enterprise_or_schema)
    product_abc_records = []
    async with my_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            # 1. Obtener los años con movimientos
            await cursor.execute(GET_DISTINCT_YEARS_MOVEMENTS(schema))
            years = [row['year'] for row in await cursor.fetchall()]

            # Si no se actualiza todo, obtener last_update por producto y año
            years_to_update = set(years)
            last_updates = {}
            if not ENV_UPDATE_ALL_DATES:
                await cursor.execute(
                    "SELECT product_id, year, last_update FROM product_abc WHERE enterprise=%s", (schema,)
                )
                last_updates = {(row['product_id'], row['year']): row['last_update'] for row in await cursor.fetchall()}

            for year in years:
                # 2. Obtener el total de ventas y utilidad del año
                await cursor.execute(GET_TOTAL_AMOUNT_AND_TOTAL_PROFIT_BY_YEAR(schema))
                totals = {row['year']: row for row in await cursor.fetchall()}
                total_amount = totals.get(year, {}).get('total_amount', 0) or 0
                total_profit = totals.get(year, {}).get('total_profit', 0) or 0

                # 3. Obtener resumen de ventas por producto para el año
                await cursor.execute(GET_PRODUCTS_SALES_SUMMARY_BY_YEAR(schema, year))
                products = await cursor.fetchall()

                # 4. Calcular porcentajes y clasificaciones
                products_with_sales = [p for p in products if p['total_amount'] not in (None, 0)]
                products_no_sales = [p for p in products if p['total_amount'] in (None, 0)]

                products_with_sales_sorted = sorted(
                    products_with_sales,
                    key=lambda x: -x['total_amount']
                )
                products_no_sales_sorted = sorted(
                    products_no_sales,
                    key=lambda x: x['description']
                )

                products_sorted = products_with_sales_sorted + products_no_sales_sorted

                acc_sales = 0
                acc_profit = 0
                for prod in products_sorted:
                    # Si no se actualiza todo, verifica si ya está actualizado
                    if not ENV_UPDATE_ALL_DATES:
                        last_update = last_updates.get((prod['product_id'], year))
                        # Solo actualiza si el año de last_update es igual al año actual
                        if last_update and hasattr(last_update, 'year') and last_update.year != year:
                            continue

                    if prod['total_amount'] in (None, 0):
                        sales_percentage = 0
                        profit_percentage = 0
                        acc_sales = 100
                        acc_profit = 100
                        sold_abc = "C"
                        profit_abc = "C"
                    else:
                        sales_percentage = float(prod['total_amount'] or 0) / float(total_amount) * 100 if total_amount > 0 else 0
                        profit_percentage = float(prod['total_profit'] or 0) / float(total_profit) * 100 if total_profit > 0 else 0
                        acc_sales += sales_percentage
                        acc_profit += profit_percentage

                        if acc_sales <= 80:
                            sold_abc = "A"
                        elif acc_sales <= 95:
                            sold_abc = "B"
                        else:
                            sold_abc = "C"

                        if acc_profit <= 80:
                            profit_abc = "A"
                        elif acc_profit <= 95:
                            profit_abc = "B"
                        else:
                            profit_abc = "C"

                    top_products = "AA" if sold_abc == "A" and profit_abc == "A" else None

                    product_abc_records.append({
                        "product_id": prod['product_id'],
                        "sales_percentage": round(sales_percentage, 5),
                        "acc_sales_percentage": round(acc_sales, 5),
                        "sold_abc": sold_abc,
                        "profit_percentage": round(profit_percentage, 5),
                        "acc_profit_percentage": round(acc_profit, 5),
                        "profit_abc": profit_abc,
                        "top_products": top_products,
                        "enterprise": schema,
                        "year": year,
                        "last_update": datetime.datetime.now(),
                    })

    # Solo upsert los registros que cumplen la condición de actualización
    if product_abc_records:
        await upsert_product_abc(my_pool, product_abc_records)

    # Siempre calcula y actualiza para la empresa "TODO"
    await calculate_product_abc_todo(my_pool)

    return product_abc_records

async def calculate_product_abc_todo(my_pool):
    product_abc_records = []

    async with my_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            # Obtener todos los años con movimientos (sin filtrar por empresa)
            await cursor.execute(GET_DISTINCT_YEARS_MOVEMENTS_ALL())
            years = [row['year'] for row in await cursor.fetchall()]

            for year in years:
                # Totales del año
                await cursor.execute(GET_TOTAL_AMOUNT_AND_TOTAL_PROFIT_BY_YEAR_ALL())
                totals = {row['year']: row for row in await cursor.fetchall()}
                total_amount = totals.get(year, {}).get('total_amount', 0) or 0
                total_profit = totals.get(year, {}).get('total_profit', 0) or 0

                # Productos del año
                await cursor.execute(GET_PRODUCTS_SALES_SUMMARY_BY_YEAR_ALL(year))
                products = await cursor.fetchall()

                # Separar productos con ventas y sin ventas
                products_with_sales = [p for p in products if (p.get('total_amount') or 0) > 0]
                products_no_sales = [p for p in products if not (p.get('total_amount') or 0)]

                # Ordenar ambos grupos
                products_with_sales_sorted = sorted(
                    products_with_sales,
                    key=lambda x: -(x.get('total_amount') or 0)
                )
                products_no_sales_sorted = sorted(
                    products_no_sales,
                    key=lambda x: x.get('description') or ''
                )

                # Concatenar para el orden final
                products_sorted = products_with_sales_sorted + products_no_sales_sorted

                acc_sales = 0
                acc_profit = 0
                for prod in products_sorted:
                    total_amount_val = prod.get('total_amount') or 0
                    total_profit_val = prod.get('total_profit') or 0
                    if total_amount_val == 0:
                        sales_percentage = 0
                        profit_percentage = 0
                        acc_sales = 100
                        acc_profit = 100
                        sold_abc = "C"
                        profit_abc = "C"
                    else:
                        sales_percentage = float(total_amount_val) / float(total_amount) * 100 if total_amount > 0 else 0
                        profit_percentage = float(total_profit_val) / float(total_profit) * 100 if total_profit > 0 else 0
                        acc_sales += sales_percentage
                        acc_profit += profit_percentage

                        if acc_sales <= 80:
                            sold_abc = "A"
                        elif acc_sales <= 95:
                            sold_abc = "B"
                        else:
                            sold_abc = "C"

                        if acc_profit <= 80:
                            profit_abc = "A"
                        elif acc_profit <= 95:
                            profit_abc = "B"
                        else:
                            profit_abc = "C"

                    top_products = "AA" if sold_abc == "A" and profit_abc == "A" else None

                    product_abc_records.append({
                        "product_id": prod['product_id'],
                        "sales_percentage": round(sales_percentage, 5),
                        "acc_sales_percentage": round(acc_sales, 5),
                        "sold_abc": sold_abc,
                        "profit_percentage": round(profit_percentage, 5),
                        "acc_profit_percentage": round(acc_profit, 5),
                        "profit_abc": profit_abc,
                        "top_products": top_products,
                        "enterprise": "TODO",
                        "year": year,
                        "last_update": datetime.datetime.now(),
                    })

    await upsert_product_abc(my_pool, product_abc_records)

async def upsert_product_abc(my_pool, product_abc_records):
    """
    Inserta o actualiza los registros de ProductABC en la base de datos MySQL.
    """
    if not product_abc_records:
        return 0

    my_query = UPSERT_PRODUCT_ABC()

    async with my_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.executemany(my_query, product_abc_records)
        await conn.commit()
    return len(product_abc_records)



### Analysis ABC
async def calculate_analysis_abc(my_pool, enterprise_or_schema: str):
    """
    Calcula y actualiza la tabla AnalysisABC agrupando por empresa, año, catálogo, familia y subfamilia,
    siguiendo las reglas de negocio y lógica de actualización descritas.
    """
    schema = get_schema_from_enterprise(enterprise_or_schema)
    analysis_abc_records = []
    async with my_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            # 1. Traer todos los años de movimientos
            await cursor.execute("SELECT DISTINCT YEAR(movement_detail_date) AS year FROM movements_detail")
            years = [row['year'] for row in await cursor.fetchall() if row['year'] is not None]

            # 2. Si no se actualiza todo, obtener last_update por agrupación
            last_updates = {}
            if not ENV_UPDATE_ALL_DATES and schema != "TODO":
                await cursor.execute(
                    "SELECT catalog_id, family_id, subfamily_id, year, last_update FROM analysis_abc WHERE enterprise=%s",
                    (schema,)
                )
                last_updates = {
                    (row['catalog_id'], row['family_id'], row['subfamily_id'], row['year']): row['last_update']
                    for row in await cursor.fetchall()
                }

            for year in years:
                # 2.1 Traer todas las agrupaciones válidas para ese año
                await cursor.execute(
                    "SELECT DISTINCT catalog_id, family_id, subfamily_id FROM product WHERE enterprise=%s AND YEAR(create_date)<=%s",
                    (schema, year)
                )
                groups = await cursor.fetchall()

                for group in groups:
                    catalog_id = group['catalog_id']
                    family_id = group['family_id']
                    subfamily_id = group['subfamily_id']

                    # Ignorar productos sin catalogo, familia o subfamilia
                    if catalog_id is None or family_id is None or subfamily_id is None:
                        continue

                    # 2.2 Si no se actualiza todo, verifica si ya está actualizado
                    if not ENV_UPDATE_ALL_DATES and schema != "TODO":
                        last_update = last_updates.get((catalog_id, family_id, subfamily_id, year))
                        if last_update and hasattr(last_update, 'year') and last_update.year > year:
                            continue

                    # 3. Calcular los datos agregados
                    # Total amount, profit, units_sold
                    await cursor.execute(
                        """
                        SELECT
                          COALESCE(SUM(md.amount),0) AS total_amount,
                          COALESCE(SUM(md.amount - (md.cost_of_sale * md.quantity)),0) AS profit,
                          COALESCE(SUM(md.quantity),0) AS units_sold
                        FROM movements_detail md
                        JOIN product p ON md.product_id = p.id
                        WHERE p.catalog_id = %s AND p.family_id = %s AND p.subfamily_id = %s
                          AND p.enterprise = %s AND YEAR(md.movement_detail_date) = %s
                        """,
                        (catalog_id, family_id, subfamily_id, schema, year)
                    )
                    row = await cursor.fetchone() or {}
                    total_amount = float(row.get('total_amount', 0) or 0)
                    profit = float(row.get('profit', 0) or 0)
                    units_sold = float(row.get('units_sold', 0) or 0)
                    profit_percentage = (profit / total_amount * 100) if total_amount else 0

                    # Inventario de cierre (unidades y pesos)
                    await cursor.execute(
                        """
                        SELECT
                          COALESCE(SUM(CASE WHEN m.is_input = true THEN md.quantity WHEN m.is_output = true THEN -md.quantity END),0) AS inventory_close_u,
                          COALESCE(SUM(CASE WHEN m.is_input = true THEN md.quantity*md.cost_of_sale WHEN m.is_output = true THEN -md.quantity*md.cost_of_sale END),0) AS inventory_close_p
                        FROM movements_detail md
                        JOIN movements m ON md.movement_id = m.id
                        JOIN product p ON md.product_id = p.id
                        WHERE p.catalog_id = %s AND p.family_id = %s AND p.subfamily_id = %s
                          AND p.enterprise = %s AND YEAR(md.movement_detail_date) <= %s
                        """,
                        (catalog_id, family_id, subfamily_id, schema, year)
                    )
                    row = await cursor.fetchone() or {}
                    inventory_close_u = float(row.get('inventory_close_u', 0) or 0)
                    inventory_close_p = float(row.get('inventory_close_p', 0) or 0)

                    # Por mes: ventas y cierre inventario
                    month_sales_u = {}
                    month_sales_p = {}
                    inventory_close_u_month = {}
                    inventory_close_p_month = {}
                    for month in range(1, 13):
                        await cursor.execute(
                            """
                            SELECT
                              COALESCE(SUM(md.quantity),0) AS month_sale_u,
                              COALESCE(SUM(md.amount),0) AS month_sale_p
                            FROM movements_detail md
                            JOIN product p ON md.product_id = p.id
                            WHERE p.catalog_id = %s AND p.family_id = %s AND p.subfamily_id = %s
                              AND p.enterprise = %s AND YEAR(md.movement_detail_date) = %s AND MONTH(md.movement_detail_date) = %s
                            """,
                            (catalog_id, family_id, subfamily_id, schema, year, month)
                        )
                        row = await cursor.fetchone() or {}
                        month_sales_u[month] = float(row.get('month_sale_u', 0) or 0)
                        month_sales_p[month] = float(row.get('month_sale_p', 0) or 0)
                        await cursor.execute(
                            """
                            SELECT
                                COALESCE(SUM(CASE WHEN m.is_input = true THEN md.quantity WHEN m.is_output = true THEN -md.quantity END),0) AS inventory_close_u,
                                COALESCE(SUM(CASE WHEN m.is_input = true THEN md.quantity*md.cost_of_sale WHEN m.is_output = true THEN -md.quantity*md.cost_of_sale END),0) AS inventory_close_p
                            FROM movements_detail md
                            JOIN movements m ON md.movement_id = m.id
                            JOIN product p ON md.product_id = p.id
                            WHERE p.catalog_id = %s AND p.family_id = %s AND p.subfamily_id = %s
                              AND p.enterprise = %s AND (YEAR(md.movement_detail_date) < %s OR (YEAR(md.movement_detail_date) = %s AND MONTH(md.movement_detail_date) <= %s))
                            """,
                            (catalog_id, family_id, subfamily_id, schema, year, year, month)
                        )
                        row = await cursor.fetchone() or {}
                        inventory_close_u_month[month] = float(row.get('inventory_close_u', 0) or 0)
                        inventory_close_p_month[month] = float(row.get('inventory_close_p', 0) or 0)

                    sold_average_month = total_amount / 12 if total_amount else 0
                    profit_average_month = profit / 12 if profit else 0
                    actual_inventory = inventory_close_p
                    average_selling_cost = (total_amount - profit) / 12 if total_amount else 0
                    inventory_average_u = sum(inventory_close_u_month.values()) / 12
                    inventory_average_p = sum(inventory_close_p_month.values()) / 12
                    inventory_days = (actual_inventory / average_selling_cost * 30) if average_selling_cost else 0
                    monthly_roi = profit_average_month / inventory_average_p if inventory_average_p else 0

                    analysis_abc_records.append({
                        "catalog_id": catalog_id,
                        "family_id": family_id,
                        "subfamily_id": subfamily_id,
                        "total_amount": total_amount,
                        "profit": profit,
                        "profit_percentage": profit_percentage,
                        "units_sold": units_sold,
                        "inventory_close_u": inventory_close_u,
                        "inventory_close_p": inventory_close_p,
                        "sold_average_month": sold_average_month,
                        "profit_average_month": profit_average_month,
                        "actual_inventory": actual_inventory,
                        "average_selling_cost": average_selling_cost,
                        "inventory_average_u": inventory_average_u,
                        "inventory_average_p": inventory_average_p,
                        "inventory_days": inventory_days,
                        "monthly_roi": monthly_roi,
                        # Meses
                        "month_sale_u_january": month_sales_u[1],
                        "month_sale_p_january": month_sales_p[1],
                        "inventory_close_u_january": inventory_close_u_month[1],
                        "inventory_close_p_january": inventory_close_p_month[1],
                        "month_sale_u_february": month_sales_u[2],
                        "month_sale_p_february": month_sales_p[2],
                        "inventory_close_u_february": inventory_close_u_month[2],
                        "inventory_close_p_february": inventory_close_p_month[2],
                        "month_sale_u_march": month_sales_u[3],
                        "month_sale_p_march": month_sales_p[3],
                        "inventory_close_u_march": inventory_close_u_month[3],
                        "inventory_close_p_march": inventory_close_p_month[3],
                        "month_sale_u_april": month_sales_u[4],
                        "month_sale_p_april": month_sales_p[4],
                        "inventory_close_u_april": inventory_close_u_month[4],
                        "inventory_close_p_april": inventory_close_p_month[4],
                        "month_sale_u_may": month_sales_u[5],
                        "month_sale_p_may": month_sales_p[5],
                        "inventory_close_u_may": inventory_close_u_month[5],
                        "inventory_close_p_may": inventory_close_p_month[5],
                        "month_sale_u_june": month_sales_u[6],
                        "month_sale_p_june": month_sales_p[6],
                        "inventory_close_u_june": inventory_close_u_month[6],
                        "inventory_close_p_june": inventory_close_p_month[6],
                        "month_sale_u_july": month_sales_u[7],
                        "month_sale_p_july": month_sales_p[7],
                        "inventory_close_u_july": inventory_close_u_month[7],
                        "inventory_close_p_july": inventory_close_p_month[7],
                        "month_sale_u_august": month_sales_u[8],
                        "month_sale_p_august": month_sales_p[8],
                        "inventory_close_u_august": inventory_close_u_month[8],
                        "inventory_close_p_august": inventory_close_p_month[8],
                        "month_sale_u_september": month_sales_u[9],
                        "month_sale_p_september": month_sales_p[9],
                        "inventory_close_u_september": inventory_close_u_month[9],
                        "inventory_close_p_september": inventory_close_p_month[9],
                        "month_sale_u_october": month_sales_u[10],
                        "month_sale_p_october": month_sales_p[10],
                        "inventory_close_u_october": inventory_close_u_month[10],
                        "inventory_close_p_october": inventory_close_p_month[10],
                        "month_sale_u_november": month_sales_u[11],
                        "month_sale_p_november": month_sales_p[11],
                        "inventory_close_u_november": inventory_close_u_month[11],
                        "inventory_close_p_november": inventory_close_p_month[11],
                        "month_sale_u_december": month_sales_u[12],
                        "month_sale_p_december": month_sales_p[12],
                        "inventory_close_u_december": inventory_close_u_month[12],
                        "inventory_close_p_december": inventory_close_p_month[12],
                        "enterprise": schema,
                        "year": year,
                        "last_update": datetime.datetime.now(),
                        # Los siguientes campos se calculan después
                        "sales_percentage": 0,
                        "acc_sales_percentage": 0,
                        "sold_abc": None,
                        "acc_profit_percentage": 0,
                        "profit_abc": None,
                        "top_products": None,
                    })

    # Calcular sales_percentage, acc_sales_percentage, sold_abc, acc_profit_percentage, profit_abc, top_products
    grouped = defaultdict(list)
    for rec in analysis_abc_records:
        grouped[(rec['enterprise'], rec['year'])].append(rec)

    for (enterprise, year), records in grouped.items():
        # Ordenar primero los que tienen ventas (total_amount > 0), luego los que no tienen ventas, igual que en product_abc
        records_with_sales = [r for r in records if r['total_amount'] > 0]
        records_no_sales = [r for r in records if r['total_amount'] <= 0]
        records_with_sales_sorted = sorted(records_with_sales, key=lambda r: -r['total_amount'])
        records_no_sales_sorted = sorted(records_no_sales, key=lambda r: (r['catalog_id'], r['family_id'], r['subfamily_id']))
        records_sorted = records_with_sales_sorted + records_no_sales_sorted

        total_amount_sum = sum(r['total_amount'] for r in records_with_sales)
        total_profit_sum = sum(r['profit'] for r in records_with_sales)
        acc_sales = 0
        acc_profit = 0
        for rec in records_sorted:
            if rec['total_amount'] > 0:
                rec['sales_percentage'] = float(rec['total_amount']) / total_amount_sum * 100 if total_amount_sum > 0 else 0
                rec['profit_percentage'] = float(rec['profit']) / total_profit_sum * 100 if total_profit_sum > 0 else 0
                acc_sales += rec['sales_percentage']
                acc_profit += rec['profit_percentage']
                rec['acc_sales_percentage'] = acc_sales
                rec['acc_profit_percentage'] = acc_profit
                if acc_sales <= 80:
                    rec['sold_abc'] = 'A'
                elif acc_sales <= 95:
                    rec['sold_abc'] = 'B'
                else:
                    rec['sold_abc'] = 'C'
                if acc_profit <= 80:
                    rec['profit_abc'] = 'A'
                elif acc_profit <= 95:
                    rec['profit_abc'] = 'B'
                else:
                    rec['profit_abc'] = 'C'
                rec['top_products'] = 'AA' if rec['sold_abc'] == 'A' and rec['profit_abc'] == 'A' else None
            else:
                rec['sales_percentage'] = 0
                rec['profit_percentage'] = 0
                rec['acc_sales_percentage'] = 100
                rec['acc_profit_percentage'] = 100
                rec['sold_abc'] = 'C'
                rec['profit_abc'] = 'C'
                rec['top_products'] = None

    # Upsert en la tabla analysis_abc
    if analysis_abc_records:
        await upsert_analysis_abc(my_pool, analysis_abc_records)

    # Si la empresa no es TODO, ejecuta también para TODO
    if schema != "TODO":
        await calculate_analysis_abc(my_pool, "TODO")


async def upsert_analysis_abc(my_pool, analysis_abc_records):
    """
    Inserta o actualiza los registros de AnalysisABC en la base de datos MySQL.
    """
    if not analysis_abc_records:
        return 0

    my_query = UPSERT_ANALYSIS_ABC()

    async with my_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.executemany(my_query, analysis_abc_records)
        await conn.commit()
    return len(analysis_abc_records)

async def calculate_analysis_abc_todo(my_pool):
    """
    Calcula y actualiza la tabla AnalysisABC global (enterprise='TODO'), agrupando por año, catálogo, familia y subfamilia,
    usando todos los movimientos y productos de todas las empresas (sin filtrar por enterprise).
    """
    analysis_abc_records = []
    async with my_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            # 1. Traer todos los años de movimientos (sin filtrar por empresa)
            await cursor.execute("SELECT DISTINCT YEAR(movement_detail_date) AS year FROM movements_detail")
            years = [row['year'] for row in await cursor.fetchall() if row['year'] is not None]

            for year in years:
                # 2. Traer todas las agrupaciones válidas para ese año (sin filtrar por empresa)
                await cursor.execute(
                    "SELECT DISTINCT catalog_id, family_id, subfamily_id FROM product WHERE YEAR(create_date)<=%s",
                    (year,)
                )
                groups = await cursor.fetchall()

                for group in groups:
                    catalog_id = group['catalog_id']
                    family_id = group['family_id']
                    subfamily_id = group['subfamily_id']

                    # Ignorar productos sin catalogo, familia o subfamilia
                    if catalog_id is None or family_id is None or subfamily_id is None:
                        continue

                    # 3. Calcular los datos agregados (sin filtrar por empresa)
                    # Total amount, profit, units_sold
                    await cursor.execute(
                        """
                        SELECT
                          COALESCE(SUM(md.amount),0) AS total_amount,
                          COALESCE(SUM(md.amount - (md.cost_of_sale * md.quantity)),0) AS profit,
                          COALESCE(SUM(md.quantity),0) AS units_sold
                        FROM movements_detail md
                        JOIN product p ON md.product_id = p.id
                        WHERE p.catalog_id = %s AND p.family_id = %s AND p.subfamily_id = %s
                          AND YEAR(md.movement_detail_date) = %s
                        """,
                        (catalog_id, family_id, subfamily_id, year)
                    )
                    row = await cursor.fetchone() or {}
                    total_amount = float(row.get('total_amount', 0) or 0)
                    profit = float(row.get('profit', 0) or 0)
                    units_sold = float(row.get('units_sold', 0) or 0)
                    profit_percentage = (profit / total_amount * 100) if total_amount else 0

                    # Inventario de cierre (unidades y pesos)
                    await cursor.execute(
                        """
                        SELECT
                          COALESCE(SUM(CASE WHEN m.is_input = true THEN md.quantity WHEN m.is_output = true THEN -md.quantity END),0) AS inventory_close_u,
                          COALESCE(SUM(CASE WHEN m.is_input = true THEN md.quantity*md.cost_of_sale WHEN m.is_output = true THEN -md.quantity*md.cost_of_sale END),0) AS inventory_close_p
                        FROM movements_detail md
                        JOIN movements m ON md.movement_id = m.id
                        JOIN product p ON md.product_id = p.id
                        WHERE p.catalog_id = %s AND p.family_id = %s AND p.subfamily_id = %s
                          AND YEAR(md.movement_detail_date) <= %s
                        """,
                        (catalog_id, family_id, subfamily_id, year)
                    )
                    row = await cursor.fetchone() or {}
                    inventory_close_u = float(row.get('inventory_close_u', 0) or 0)
                    inventory_close_p = float(row.get('inventory_close_p', 0) or 0)

                    # Por mes: ventas y cierre inventario
                    month_sales_u = {}
                    month_sales_p = {}
                    inventory_close_u_month = {}
                    inventory_close_p_month = {}
                    for month in range(1, 13):
                        await cursor.execute(
                            """
                            SELECT
                              COALESCE(SUM(md.quantity),0) AS month_sale_u,
                              COALESCE(SUM(md.amount),0) AS month_sale_p
                            FROM movements_detail md
                            JOIN product p ON md.product_id = p.id
                            WHERE p.catalog_id = %s AND p.family_id = %s AND p.subfamily_id = %s
                              AND YEAR(md.movement_detail_date) = %s AND MONTH(md.movement_detail_date) = %s
                            """,
                            (catalog_id, family_id, subfamily_id, year, month)
                        )
                        row = await cursor.fetchone() or {}
                        month_sales_u[month] = float(row.get('month_sale_u', 0) or 0)
                        month_sales_p[month] = float(row.get('month_sale_p', 0) or 0)
                        await cursor.execute(
                            """
                            SELECT
                                COALESCE(SUM(CASE WHEN m.is_input = true THEN md.quantity WHEN m.is_output = true THEN -md.quantity END),0) AS inventory_close_u,
                                COALESCE(SUM(CASE WHEN m.is_input = true THEN md.quantity*md.cost_of_sale WHEN m.is_output = true THEN -md.quantity*md.cost_of_sale END),0) AS inventory_close_p
                            FROM movements_detail md
                            JOIN movements m ON md.movement_id = m.id
                            JOIN product p ON md.product_id = p.id
                            WHERE p.catalog_id = %s AND p.family_id = %s AND p.subfamily_id = %s
                              AND (YEAR(md.movement_detail_date) < %s OR (YEAR(md.movement_detail_date) = %s AND MONTH(md.movement_detail_date) <= %s))
                            """,
                            (catalog_id, family_id, subfamily_id, year, year, month)
                        )
                        row = await cursor.fetchone() or {}
                        inventory_close_u_month[month] = float(row.get('inventory_close_u', 0) or 0)
                        inventory_close_p_month[month] = float(row.get('inventory_close_p', 0) or 0)

                    sold_average_month = total_amount / 12 if total_amount else 0
                    profit_average_month = profit / 12 if profit else 0
                    actual_inventory = inventory_close_p
                    average_selling_cost = (total_amount - profit) / 12 if total_amount else 0
                    inventory_average_u = sum(inventory_close_u_month.values()) / 12
                    inventory_average_p = sum(inventory_close_p_month.values()) / 12
                    inventory_days = (actual_inventory / average_selling_cost * 30) if average_selling_cost else 0
                    monthly_roi = profit_average_month / inventory_average_p if inventory_average_p else 0

                    analysis_abc_records.append({
                        "catalog_id": catalog_id,
                        "family_id": family_id,
                        "subfamily_id": subfamily_id,
                        "total_amount": total_amount,
                        "profit": profit,
                        "profit_percentage": profit_percentage,
                        "units_sold": units_sold,
                        "inventory_close_u": inventory_close_u,
                        "inventory_close_p": inventory_close_p,
                        "sold_average_month": sold_average_month,
                        "profit_average_month": profit_average_month,
                        "actual_inventory": actual_inventory,
                        "average_selling_cost": average_selling_cost,
                        "inventory_average_u": inventory_average_u,
                        "inventory_average_p": inventory_average_p,
                        "inventory_days": inventory_days,
                        "monthly_roi": monthly_roi,
                        # Meses
                        "month_sale_u_january": month_sales_u[1],
                        "month_sale_p_january": month_sales_p[1],
                        "inventory_close_u_january": inventory_close_u_month[1],
                        "inventory_close_p_january": inventory_close_p_month[1],
                        "month_sale_u_february": month_sales_u[2],
                        "month_sale_p_february": month_sales_p[2],
                        "inventory_close_u_february": inventory_close_u_month[2],
                        "inventory_close_p_february": inventory_close_p_month[2],
                        "month_sale_u_march": month_sales_u[3],
                        "month_sale_p_march": month_sales_p[3],
                        "inventory_close_u_march": inventory_close_u_month[3],
                        "inventory_close_p_march": inventory_close_p_month[3],
                        "month_sale_u_april": month_sales_u[4],
                        "month_sale_p_april": month_sales_p[4],
                        "inventory_close_u_april": inventory_close_u_month[4],
                        "inventory_close_p_april": inventory_close_p_month[4],
                        "month_sale_u_may": month_sales_u[5],
                        "month_sale_p_may": month_sales_p[5],
                        "inventory_close_u_may": inventory_close_u_month[5],
                        "inventory_close_p_may": inventory_close_p_month[5],
                        "month_sale_u_june": month_sales_u[6],
                        "month_sale_p_june": month_sales_p[6],
                        "inventory_close_u_june": inventory_close_u_month[6],
                        "inventory_close_p_june": inventory_close_p_month[6],
                        "month_sale_u_july": month_sales_u[7],
                        "month_sale_p_july": month_sales_p[7],
                        "inventory_close_u_july": inventory_close_u_month[7],
                        "inventory_close_p_july": inventory_close_p_month[7],
                        "month_sale_u_august": month_sales_u[8],
                        "month_sale_p_august": month_sales_p[8],
                        "inventory_close_u_august": inventory_close_u_month[8],
                        "inventory_close_p_august": inventory_close_p_month[8],
                        "month_sale_u_september": month_sales_u[9],
                        "month_sale_p_september": month_sales_p[9],
                        "inventory_close_u_september": inventory_close_u_month[9],
                        "inventory_close_p_september": inventory_close_p_month[9],
                        "month_sale_u_october": month_sales_u[10],
                        "month_sale_p_october": month_sales_p[10],
                        "inventory_close_u_october": inventory_close_u_month[10],
                        "inventory_close_p_october": inventory_close_p_month[10],
                        "month_sale_u_november": month_sales_u[11],
                        "month_sale_p_november": month_sales_p[11],
                        "inventory_close_u_november": inventory_close_u_month[11],
                        "inventory_close_p_november": inventory_close_p_month[11],
                        "month_sale_u_december": month_sales_u[12],
                        "month_sale_p_december": month_sales_p[12],
                        "inventory_close_u_december": inventory_close_u_month[12],
                        "inventory_close_p_december": inventory_close_p_month[12],
                        "enterprise": "TODO",
                        "year": year,
                        "last_update": datetime.datetime.now(),
                        # Los siguientes campos se calculan después
                        "sales_percentage": 0,
                        "acc_sales_percentage": 0,
                        "sold_abc": None,
                        "acc_profit_percentage": 0,
                        "profit_abc": None,
                        "top_products": None,
                    })

    # Calcular sales_percentage, acc_sales_percentage, sold_abc, acc_profit_percentage, profit_abc, top_products
    grouped = defaultdict(list)
    for rec in analysis_abc_records:
        grouped[(rec['enterprise'], rec['year'])].append(rec)

    for (enterprise, year), records in grouped.items():
        records_with_sales = [r for r in records if r['total_amount'] > 0]
        records_no_sales = [r for r in records if r['total_amount'] <= 0]
        records_with_sales_sorted = sorted(records_with_sales, key=lambda r: -r['total_amount'])
        records_no_sales_sorted = sorted(records_no_sales, key=lambda r: (r['catalog_id'], r['family_id'], r['subfamily_id']))
        records_sorted = records_with_sales_sorted + records_no_sales_sorted

        total_amount_sum = sum(r['total_amount'] for r in records_with_sales)
        total_profit_sum = sum(r['profit'] for r in records_with_sales)
        acc_sales = 0
        acc_profit = 0
        for rec in records_sorted:
            if rec['total_amount'] > 0:
                rec['sales_percentage'] = float(rec['total_amount']) / total_amount_sum * 100 if total_amount_sum > 0 else 0
                rec['profit_percentage'] = float(rec['profit']) / total_profit_sum * 100 if total_profit_sum > 0 else 0
                acc_sales += rec['sales_percentage']
                acc_profit += rec['profit_percentage']
                rec['acc_sales_percentage'] = acc_sales
                rec['acc_profit_percentage'] = acc_profit
                if acc_sales <= 80:
                    rec['sold_abc'] = 'A'
                elif acc_sales <= 95:
                    rec['sold_abc'] = 'B'
                else:
                    rec['sold_abc'] = 'C'
                if acc_profit <= 80:
                    rec['profit_abc'] = 'A'
                elif acc_profit <= 95:
                    rec['profit_abc'] = 'B'
                else:
                    rec['profit_abc'] = 'C'
                rec['top_products'] = 'AA' if rec['sold_abc'] == 'A' and rec['profit_abc'] == 'A' else None
            else:
                rec['sales_percentage'] = 0
                rec['profit_percentage'] = 0
                rec['acc_sales_percentage'] = 100
                rec['acc_profit_percentage'] = 100
                rec['sold_abc'] = 'C'
                rec['profit_abc'] = 'C'
                rec['top_products'] = None

    # Upsert en la tabla analysis_abc
    if analysis_abc_records:
        await upsert_analysis_abc(my_pool, analysis_abc_records)

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
