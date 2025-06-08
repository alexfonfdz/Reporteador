# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from asyncpg.connection import asyncio
from decouple import os
from django.test import TestCase
from apps.home.product_abc_logic import refresh_data

# Create your tests here.

class TestInsertingLogic(TestCase):

    def test_a_refresh_data(self):
        catalog_path = os.path.join('apps', 'static', 'data', 'Catalogo.para.agrupaciones.MARW-Archivo_limpio.xlsx')
        catalog_column_name = 'Catalogo'
 
        asyncio.run(
            refresh_data(
                enterprise="MR DIESEL",
                catalog_path=catalog_path,
                catalog_name_column=catalog_column_name
            )
        )
       


