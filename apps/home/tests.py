# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from django.test import TestCase, Client
from datetime import datetime
import mysql.connector as m
from core.settings import ENV_MYSQL_NAME, ENV_MYSQL_USER, ENV_MYSQL_HOST, ENV_MYSQL_PORT, ENV_MYSQL_PASSWORD

# Create your tests here.

class InsertDataTestCase(TestCase):

    def setUp(self) -> None:
        self.c = Client()

    def testA_insert_families_and_subfamilies(self):
        print("===================================================================================================")
        print("INSERTANDO FAMILIAS Y SUBFAMILIAS")
        start = datetime.now()
        self.c.post('/get_families_from_admintotal')
        end = datetime.now()

        difference = end - start

        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD, database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)

        cursor = conn.cursor()

        cursor.execute("SELECT Count(*) FROM family")

        family_inserted = cursor.fetchall()

        cursor.execute("SELECT Count(*) FROM subfamily")
        subfamily_inserted = cursor.fetchall()

        conn.commit()
        cursor.close()
        conn.close()

        print(f'family insertions {family_inserted}')
        print(f'subfamily insertions {subfamily_inserted}')

        print(f'Took {difference.total_seconds()} seconds to insert families and subfamilies')

    def testB_insert_products(self):
        print("===================================================================================================")
        print("INSERTANDO PRODUCTOS")

        start = datetime.now()
        self.c.post('/get_products_from_admintotal')
        end = datetime.now()

        difference = end - start
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD, database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)

        cursor = conn.cursor()

        cursor.execute("SELECT Count(*) FROM product")
        products_insertion = cursor.fetchall()

        conn.commit()
        cursor.close()
        conn.close()

        print(f'product insertions {products_insertion}')

        print(f'Took {difference.total_seconds()} seconds to insert products')

    def testC_insert_catalog(self):
        print("===================================================================================================")
        print("INSERTANDO CATALOGO")


        start = datetime.now()
        response = self.c.post('/get_catalogs_from_admintotal')
        end = datetime.now()

        difference = end - start
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD, database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)

        cursor = conn.cursor()

        cursor.execute("SELECT Count(*) FROM catalog")
        catalog_insertion = cursor.fetchall()

        conn.commit()
        cursor.close()
        conn.close()

        print(f'catalog insertions {catalog_insertion}')

        print(f'Took {difference.total_seconds()} seconds to insert catalog')

    def testD_insert_product_catalog(self):
        print("===================================================================================================")
        print("INSERTANDO PRODUCTO-CATALOGO")


        start = datetime.now()
        response = self.c.post('/insert_product_catalog', {"year": 2024}, content_type='application/json')
        end = datetime.now()

        difference = end - start
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD, database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)

        cursor = conn.cursor()

        cursor.execute("SELECT Count(*) FROM product_catalog")
        catalog_insertion = cursor.fetchall()

        conn.commit()
        cursor.close()
        conn.close()

        print(f'product_catalog insertions {catalog_insertion}')

        print(f'Took {difference.total_seconds()} seconds to insert product_catalog')

    def testE_insert_data_product_abc(self):
        print("===================================================================================================")
        print("INSERTANDO DATA PRODUCTO ABC")

        start = datetime.now()
        response = self.c.post('/insert_data_to_product_abc', {"year": 2024, "enterprise": "marw"}, content_type='application/json')
        end = datetime.now()

        difference = end - start
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD, database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)

        cursor = conn.cursor()

        cursor.execute("SELECT Count(*) FROM product_abc")
        catalog_insertion = cursor.fetchall()

        conn.commit()
        cursor.close()
        conn.close()

        print(f'product_abc part 0 insertions {catalog_insertion}')

        print(f'Took {difference.total_seconds()} seconds to insert product_abc part 0')


