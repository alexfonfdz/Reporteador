# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from django.db import models
from django.contrib.auth.models import User

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
    last_update = models.DateTimeField(null=True, blank=True, default=None)

    def __str__(self):
        return f"{self.product} - {self.catalog}"
    
    class Meta:
        db_table = "product_catalog"
        verbose_name = "Product Catalog"
        verbose_name_plural = "Product Catalogs"

class ProductABC(models.Model):
    catalog = models.ForeignKey(Catalog, on_delete=models.PROTECT, related_name='abc_products')
    family = models.ForeignKey(Family, on_delete=models.PROTECT, related_name='abc_products')
    subfamily = models.ForeignKey(SubFamily, on_delete=models.PROTECT, related_name='abc_products')
    
    total_amount = models.DecimalField(max_digits=25, decimal_places=2, default=0)
    profit = models.DecimalField(max_digits=25, decimal_places=2, default=0)
    profit_percentage = models.DecimalField(max_digits=25, decimal_places=2, default=0)
    units_sold = models.IntegerField(default=0)
    inventory_close_u = models.DecimalField(max_digits=25, decimal_places=2, default=0)
    inventory_close_p = models.DecimalField(max_digits=25, decimal_places=2, default=0)
    monthly_roi = models.DecimalField(max_digits=25, decimal_places=2)
    sold_average_month = models.DecimalField(max_digits=25, decimal_places=2)
    profit_average_month = models.DecimalField(max_digits=25, decimal_places=2)
    actual_inventory = models.DecimalField(max_digits=25, decimal_places=2)
    average_selling_cost = models.DecimalField(max_digits=25, decimal_places=2)
    inventory_average_u = models.DecimalField(max_digits=25, decimal_places=2)
    inventory_average_p = models.DecimalField(max_digits=25, decimal_places=2)
    inventory_days = models.IntegerField()
    sales_percentage = models.DecimalField(max_digits=25, decimal_places=2)
    acc_sales_percentage = models.DecimalField(max_digits=25, decimal_places=2)
    sold_abc = models.CharField(max_length=1, unique=False)
    profit_percentage = models.DecimalField(max_digits=25, decimal_places=2)
    acc_profit_percentage = models.DecimalField(max_digits=25, decimal_places=2)
    profit_abc = models.CharField(max_length=1, unique=False)
    top_products = models.CharField(max_length=2, unique=False, null=True, blank=True)
    month_sale_u_january = models.DecimalField(max_digits=25, decimal_places=2)
    month_sale_p_january = models.DecimalField(max_digits=25, decimal_places=2)
    inventory_close_u_january = models.DecimalField(max_digits=25, decimal_places=2)
    inventory_close_p_january = models.DecimalField(max_digits=25, decimal_places=2)
    month_sale_u_february = models.DecimalField(max_digits=25, decimal_places=2)
    month_sale_p_february = models.DecimalField(max_digits=25, decimal_places=2)
    inventory_close_u_february = models.DecimalField(max_digits=25, decimal_places=2)
    inventory_close_p_february = models.DecimalField(max_digits=25, decimal_places=2)
    month_sale_u_march = models.DecimalField(max_digits=25, decimal_places=2)
    month_sale_p_march = models.DecimalField(max_digits=25, decimal_places=2)
    inventory_close_u_march = models.DecimalField(max_digits=25, decimal_places=2)
    inventory_close_p_march = models.DecimalField(max_digits=25, decimal_places=2)
    month_sale_u_april = models.DecimalField(max_digits=25, decimal_places=2)
    month_sale_p_april = models.DecimalField(max_digits=25, decimal_places=2)
    inventory_close_u_april = models.DecimalField(max_digits=25, decimal_places=2)
    inventory_close_p_april = models.DecimalField(max_digits=25, decimal_places=2)
    month_sale_u_may = models.DecimalField(max_digits=25, decimal_places=2)
    month_sale_p_may = models.DecimalField(max_digits=25, decimal_places=2)
    inventory_close_u_may = models.DecimalField(max_digits=25, decimal_places=2)
    inventory_close_p_may = models.DecimalField(max_digits=25, decimal_places=2)
    month_sale_u_june = models.DecimalField(max_digits=25, decimal_places=2)
    month_sale_p_june = models.DecimalField(max_digits=25, decimal_places=2)
    inventory_close_u_june = models.DecimalField(max_digits=25, decimal_places=2)
    inventory_close_p_june = models.DecimalField(max_digits=25, decimal_places=2)
    month_sale_u_july = models.DecimalField(max_digits=25, decimal_places=2)
    month_sale_p_july = models.DecimalField(max_digits=25, decimal_places=2)
    inventory_close_u_july = models.DecimalField(max_digits=25, decimal_places=2)
    inventory_close_p_july = models.DecimalField(max_digits=25, decimal_places=2)
    month_sale_u_august = models.DecimalField(max_digits=25, decimal_places=2)
    month_sale_p_august = models.DecimalField(max_digits=25, decimal_places=2)
    inventory_close_u_august = models.DecimalField(max_digits=25, decimal_places=2)
    inventory_close_p_august = models.DecimalField(max_digits=25, decimal_places=2)
    month_sale_u_september = models.DecimalField(max_digits=25, decimal_places=2)
    month_sale_p_september = models.DecimalField(max_digits=25, decimal_places=2)
    inventory_close_u_september = models.DecimalField(max_digits=25, decimal_places=2)
    inventory_close_p_september = models.DecimalField(max_digits=25, decimal_places=2)
    month_sale_u_october = models.DecimalField(max_digits=25, decimal_places=2)
    month_sale_p_october = models.DecimalField(max_digits=25, decimal_places=2)
    inventory_close_u_october = models.DecimalField(max_digits=25, decimal_places=2)
    inventory_close_p_october = models.DecimalField(max_digits=25, decimal_places=2)
    month_sale_u_november = models.DecimalField(max_digits=25, decimal_places=2)
    month_sale_p_november = models.DecimalField(max_digits=25, decimal_places=2)
    inventory_close_u_november = models.DecimalField(max_digits=25, decimal_places=2)
    inventory_close_p_november = models.DecimalField(max_digits=25, decimal_places=2)
    month_sale_u_december = models.DecimalField(max_digits=25, decimal_places=2)
    month_sale_p_december = models.DecimalField(max_digits=25, decimal_places=2)
    inventory_close_u_december = models.DecimalField(max_digits=25, decimal_places=2)
    inventory_close_p_december = models.DecimalField(max_digits=25, decimal_places=2)
    assigned_company = models.CharField(max_length=100, null=True, blank=True)
    year = models.IntegerField()
    last_update = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "product_abc"
        verbose_name = "Product ABC"
        verbose_name_plural = "Products ABC"