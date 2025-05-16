# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from django.test import TestCase, Client

# Create your tests here.

class UpsertFamiliesTestCase(TestCase):

    def setUp(self) -> None:
        self.c = Client()

    def test_executing(self):
        response = self.c.post('/upsert_families')
        print(response)

