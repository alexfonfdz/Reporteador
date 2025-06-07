# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from asyncpg.connection import asyncio
from django.test import TestCase
from apps.home.product_abc_logic import refresh_data

# Create your tests here.

class TestInsertingLogic(TestCase):

    def test_a_refresh_data(self):
        asyncio.run(refresh_data(enterprise="MR DIESEL"))
        


