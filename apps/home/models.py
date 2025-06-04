# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from django.db import models
from django.contrib.auth.models import User
from django.utils import timezone

# Create your models here.
class Family(models.Model):
    name = models.CharField(max_length=100, unique=False, blank=True)
    id_admin = models.IntegerField(unique=True, null=True, blank=True)

    def __str__(self):
        return self.name
    
    class Meta:
        db_table = "family"
        verbose_name = "Family"
        verbose_name_plural = "Families"
    
class SubFamily(models.Model):
    name = models.CharField(max_length=100, unique=False, blank=True)
    id_admin = models.IntegerField(unique=True, null=True, blank=True)

    def __str__(self):
        return self.name
    
    class Meta:
        db_table = "subfamily"
        verbose_name = "SubFamily"
        verbose_name_plural = "SubFamilies"

class Product(models.Model):
    family = models.ForeignKey(Family, on_delete=models.PROTECT, related_name='products')
    subfamily = models.ForeignKey(SubFamily, on_delete=models.PROTECT, related_name='products')
    id_admin = models.IntegerField(unique=True, null=True, blank=True)
    code = models.CharField(max_length=100, unique=True)
    description = models.TextField()

    def __str__(self):
        return self.description
    
    class Meta:
        db_table = "product"
        verbose_name = "Product"
        verbose_name_plural = "Products"

class Catalog(models.Model):
    family = models.CharField(max_length=100, unique=False, blank=True, null=True)
    subfamily = models.CharField(max_length=100, unique=False, blank=True, null=True)    
    description = models.TextField()    

    def __str__(self):
        return self.description
    
    class Meta:
        db_table = "catalog"
        verbose_name = "Catalog"
        verbose_name_plural = "Catalogs"

class ProductCatalog(models.Model):
    product = models.ForeignKey(Product, on_delete=models.PROTECT, related_name='catalogs')
    catalog = models.ForeignKey(Catalog, on_delete=models.PROTECT, related_name='products')
    add_year = models.IntegerField()
    last_update = models.DateTimeField( default=timezone.now, blank=True)

    def __str__(self):
        return f"{self.product} - {self.catalog}"
    
    class Meta:
        db_table = "product_catalog"
        verbose_name = "Product Catalog"
        verbose_name_plural = "Product Catalogs"

class ProductABC(models.Model):
    catalog = models.ForeignKey(Catalog, on_delete=models.PROTECT, related_name='abc_products')    
    
    total_amount = models.DecimalField(max_digits=25, decimal_places=2, default=0, null=True, blank=True)
    profit = models.DecimalField(max_digits=25, decimal_places=2, default=0, null=True, blank=True)
    profit_percentage = models.DecimalField(max_digits=25, decimal_places=2, default=0, null=True, blank=True)
    units_sold = models.IntegerField(default=0, null=True, blank=True)
    inventory_close_u = models.DecimalField(max_digits=25, decimal_places=2, default=0, null=True, blank=True)
    inventory_close_p = models.DecimalField(max_digits=25, decimal_places=2, default=0, null=True, blank=True)
    monthly_roi = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    sold_average_month = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    profit_average_month = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    actual_inventory = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    average_selling_cost = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    inventory_average_u = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    inventory_average_p = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    inventory_days = models.IntegerField(null=True, blank=True)
    sales_percentage = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    acc_sales_percentage = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    sold_abc = models.CharField(max_length=1, unique=False, null=True, blank=True)
    profit_percentage = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    acc_profit_percentage = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    profit_abc = models.CharField(max_length=1, unique=False, null=True, blank=True)
    top_products = models.CharField(max_length=2, unique=False, null=True, blank=True)
    month_sale_u_january = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    month_sale_p_january = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    inventory_close_u_january = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    inventory_close_p_january = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    month_sale_u_february = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    month_sale_p_february = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    inventory_close_u_february = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    inventory_close_p_february = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    month_sale_u_march = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    month_sale_p_march = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    inventory_close_u_march = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    inventory_close_p_march = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    month_sale_u_april = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    month_sale_p_april = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    inventory_close_u_april = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    inventory_close_p_april = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    month_sale_u_may = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    month_sale_p_may = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    inventory_close_u_may = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    inventory_close_p_may = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    month_sale_u_june = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    month_sale_p_june = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    inventory_close_u_june = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    inventory_close_p_june = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    month_sale_u_july = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    month_sale_p_july = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    inventory_close_u_july = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    inventory_close_p_july = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    month_sale_u_august = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    month_sale_p_august = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    inventory_close_u_august = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    inventory_close_p_august = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    month_sale_u_september = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    month_sale_p_september = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    inventory_close_u_september = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    inventory_close_p_september = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    month_sale_u_october = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    month_sale_p_october = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    inventory_close_u_october = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    inventory_close_p_october = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    month_sale_u_november = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    month_sale_p_november = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    inventory_close_u_november = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    inventory_close_p_november = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    month_sale_u_december = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    month_sale_p_december = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    inventory_close_u_december = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)
    inventory_close_p_december = models.DecimalField(max_digits=25, decimal_places=2, null=True, blank=True)

    assigned_company = models.CharField(max_length=100, null=True, blank=True)
    year = models.IntegerField()
    last_update = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "product_abc"
        verbose_name = "Product ABC"
        verbose_name_plural = "Products ABC"