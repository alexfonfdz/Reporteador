# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from asyncpg.connection import asyncio
from decouple import os
from django.test import TestCase
from apps.home.product_abc_logic import refresh_data, calculate_product_abc, calculate_product_abc_todo, calculate_analysis_abc, calculate_analysis_abc_todo, enterprises, upsert_catalog, upsert_product_catalog, delete_catalog
import aiomysql
import asyncpg
from core.settings import (
    ENV_MYSQL_USER,
    ENV_MYSQL_HOST,
    ENV_MYSQL_PORT,
    ENV_MYSQL_PASSWORD,
    ENV_MYSQL_NAME
)

# Create your tests here.

class TestInsertingLogic(TestCase):

    def test_b_upsert_catalog(self):
        catalog_path = os.path.join('apps', 'static', 'data', 'marw_catalog.xlsx')
        catalog_column_name = 'catalog'
        product_code_column = 'code'
        enterprise = enterprises.get('MR DIESEL')

        async def job():
            my_pool = await aiomysql.create_pool(
                user=ENV_MYSQL_USER,
                host=ENV_MYSQL_HOST,
                port=int(ENV_MYSQL_PORT or 3306),
                password=ENV_MYSQL_PASSWORD,
                db=ENV_MYSQL_NAME,
                minsize=3,
                maxsize=5
            )
            print("Eliminando catalogo", end="")
            await delete_catalog(my_pool)
            print()

            print("Insertando catalogo", end="")
            catalogos_insertados = await upsert_catalog(my_pool, catalog_path, catalog_column_name)
            print(f" - {catalogos_insertados}")

            print("Actaulizando catalogos en productos", end="")
            productos_actualizados = await upsert_product_catalog(my_pool, enterprise.schema, catalog_path, catalog_column_name, product_code_column)
            print(f" - {productos_actualizados}")
   
        asyncio.run(
            job()
        )
       

    def test_a_refresh_data(self):
        catalog_path = os.path.join('apps', 'static', 'data', 'Catalogo.para.agrupaciones.MARW-Archivo_limpio.xlsx')
        catalog_column_name = 'Catalogo'
        product_code_column = 'Codigo'
        enterprise = enterprises.get('MR DIESEL')

        async def job():
            my_pool = await aiomysql.create_pool(
                user=ENV_MYSQL_USER,
                host=ENV_MYSQL_HOST,
                port=int(ENV_MYSQL_PORT or 3306),
                password=ENV_MYSQL_PASSWORD,
                db=ENV_MYSQL_NAME,
                minsize=3,
                maxsize=5
            )
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
            refresh_data(
                enterprise="MR DIESEL",
                catalog_path=catalog_path,
                catalog_name_column=catalog_column_name,
                product_code_column=product_code_column,
                update_all=False,
                pg_pool=pg_pool,
                my_pool=my_pool
            )
    
        asyncio.run(
            job()
        )

class TestCalculateProductABC(TestCase):

    def test_a_calculate_product_abc(self):

        async def run_test():
            my_pool = await aiomysql.create_pool(
            user=ENV_MYSQL_USER,
            host=ENV_MYSQL_HOST,
            port=int(ENV_MYSQL_PORT or 3306),
            password=ENV_MYSQL_PASSWORD,
            db=ENV_MYSQL_NAME,
            minsize=3,
            maxsize=5
            )

            await calculate_product_abc(
                my_pool=my_pool,
                enterprise_or_schema="MR DIESEL"
            )

        asyncio.run(run_test())
       
class TestCalculateAnalysisABC(TestCase):
    def test_a_calculate_analysis_abc(self):

        async def run_test():
            my_pool = await aiomysql.create_pool(
            user=ENV_MYSQL_USER,
            host=ENV_MYSQL_HOST,
            port=int(ENV_MYSQL_PORT or 3306),
            password=ENV_MYSQL_PASSWORD,
            db=ENV_MYSQL_NAME,
            minsize=3,
            maxsize=5
            )

            await calculate_analysis_abc(
                my_pool=my_pool,
                enterprise_or_schema="MR DIESEL"
            )

        asyncio.run(run_test())

class TestCalculateAnalysisABCTodo(TestCase):
    def test_a_calculate_analysis_abc_todo(self):
        import datetime
        async def run_test():
            my_pool = await aiomysql.create_pool(
                user=ENV_MYSQL_USER,
                host=ENV_MYSQL_HOST,
                port=int(ENV_MYSQL_PORT or 3306),
                password=ENV_MYSQL_PASSWORD,
                db=ENV_MYSQL_NAME,
                minsize=3,
                maxsize=5
            )
            # Ejecutar el cálculo global
            await calculate_analysis_abc_todo(
                my_pool=my_pool
            )

        asyncio.run(run_test())
