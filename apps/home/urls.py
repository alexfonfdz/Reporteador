# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from django.urls import path, re_path
from apps.home import views

urlpatterns = [

    # The home page
    path('', views.index, name='home'),

    # No modificar
    path('getYears', views.get_years, name='getYears'),
    path('getMonths', views.get_months, name='getMonths'),
    path('getDays', views.get_days, name='getDays'),
    path('getProducts', views.get_products, name='getProducts'),
    path('getFolio', views.get_folio, name='getFolio'),
    path('getClientesNuevos', views.get_clientes_nuevos, name='getClientesNuevos'),
    path('getClientesAusentes', views.get_clientes_ausentes, name='getClientesAusentes'),
    path('getAlmacenes', views.get_almacenes, name='getAlmacenes'),
    path('getAlmacenData', views.get_almacen_data, name='getAlmacenData'),

    # Matches any html file
    re_path(r'^.*\.*', views.pages, name='pages'),

]
