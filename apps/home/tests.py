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

    def test_insert_families_and_subfamilies(self):
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

    def test_insert_products(self):
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





