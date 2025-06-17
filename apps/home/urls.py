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
    path('getDebugProductosABC', views.debug_productosabc, name='getDebugProductosABC'),
    path('getEnterprises', views.get_enterprises, name='getEnterprises'),
    path('getProductsABC', views.get_products_abc, name='getProductsABC'),
    path('getAnalysisABC', views.get_analysis_abc, name='getAnalysisABC'),
    path('getFamilies', views.get_families, name='getFamilies'),
    path('getSubfamilies', views.get_subfamilies, name='getSubfamilies'),
    path('getBrands', views.get_brands, name='getBrands'),
    path('getCatalogs', views.get_catalogs, name='getCatalogs'),

    # Matches any html file
    re_path(r'^.*\.*', views.pages, name='pages'),

]
