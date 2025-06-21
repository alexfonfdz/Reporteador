import os

from apps.home.product_abc_logic import delete_analisis_abc, delete_productos_abc, delete_catalog, refresh_data, enterprises, calculate_product_abc, calculate_analysis_abc, calculate_analysis_abc_todo, calculate_product_abc_todo
from core.settings import  ENV_MYSQL_HOST, ENV_MYSQL_PORT, ENV_MYSQL_NAME, ENV_MYSQL_USER, ENV_MYSQL_PASSWORD, ENV_UPDATE_ALL_DATES
import aiomysql
import asyncpg


catalog_column_name = 'catalog'
product_code_column_name = 'code'

async def main():
  my_pool = await aiomysql.create_pool(
      user=ENV_MYSQL_USER,
      host=ENV_MYSQL_HOST,
      port=int(ENV_MYSQL_PORT or 3306),
      password=ENV_MYSQL_PASSWORD,
      db=ENV_MYSQL_NAME,
      minsize=3,
      maxsize=5
  )
  await delete_analisis_abc(my_pool)
  print("Analisis ABC eliminado")
  await delete_productos_abc(my_pool)
  print("Productos ABC eliminado")
  await delete_catalog(my_pool)
  print("Catalogo eliminado")


  for key, enterprise in enterprises.items():
    pg_pool = await asyncpg.create_pool(
        user=enterprise.user,
        host=enterprise.host,
        port=enterprise.port,
        database=enterprise.name,
        password=enterprise.password,
        min_size=3,
        max_size=5,
        max_inactive_connection_lifetime=300
    )
    catalog_file_name = f'{enterprise.schema}_catalog.xlsx'
    catalog_file_path = os.path.join('.', 'apps', 'static', 'data', catalog_file_name)


    await refresh_data(key, catalog_file_path, catalog_column_name, product_code_column_name, ENV_UPDATE_ALL_DATES, pg_pool, my_pool)
    print(f"Datos actualizados - {enterprise.schema}")
    await pg_pool.close()

    await calculate_product_abc(my_pool=my_pool, enterprise_or_schema=enterprise.schema)
    print(f"Productos ABC calculados - {enterprise.schema}")
    await calculate_analysis_abc(my_pool=my_pool, enterprise_or_schema=enterprise.schema)
    print(f"Analisis ABC calculados - {enterprise.schema}")


  await calculate_product_abc_todo(my_pool=my_pool)
  print("Productos ABC calculados - Todos")
  await calculate_analysis_abc_todo(my_pool=my_pool)
  print("Analisis ABC calculados - Todos")

  my_pool.close()


if __name__ == '__main__':
  import asyncio
  asyncio.run(main())