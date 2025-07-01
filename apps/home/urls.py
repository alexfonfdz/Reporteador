# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from django.urls import path, re_path
from apps.home import views

urlpatterns = [

    # The home page
    path('', views.index, name='home'),

    path('getDebugProductosABC', views.debug_productosabc, name='getDebugProductosABC'),
    path('getEnterprises', views.get_enterprises, name='getEnterprises'),
    path('getProductsABC', views.get_products_abc, name='getProductsABC'),
    path('getAnalysisABC', views.get_analysis_abc, name='getAnalysisABC'),
    path('getFamilies', views.get_families, name='getFamilies'),
    path('getSubfamilies', views.get_subfamilies, name='getSubfamilies'),
    path('getBrands', views.get_brands, name='getBrands'),
    path('getCatalogs', views.get_catalogs, name='getCatalogs'),
    path('getTopProducts', views.get_product_top, name='getTopProducts'),
    path('getMinMovementsDate', views.get_min_movements_date, name='getMinMovementsDate'),
    path('getProductCatalog', views.get_product_catalog, name='getProductCatalog'),
    path('uploadCatalogFile', views.upload_catalog_file, name='uploadCatalogFile'),
    

    # Matches any html file
    re_path(r'^.*\.*', views.pages, name='pages'),

]
