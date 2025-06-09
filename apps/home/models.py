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
    id_admin = models.IntegerField()
    enterprise = models.CharField(max_length=100, unique=False, blank=False, default="none")

    def __str__(self):
        return self.name
    
    class Meta:
        db_table = "family"
        verbose_name = "Family"
        verbose_name_plural = "Families"
        constraints=[
            models.UniqueConstraint(fields=["id_admin", "enterprise"], name="enterprise_id_admin_family_unique_idx")
        ]
    
class SubFamily(models.Model):
    name = models.CharField(max_length=100, unique=False, blank=True)
    id_admin = models.IntegerField()
    enterprise = models.CharField(max_length=100, unique=False, blank=False, default="none")

    def __str__(self):
        return self.name
    
    class Meta:
        db_table = "subfamily"
        verbose_name = "SubFamily"
        verbose_name_plural = "SubFamilies"
        constraints=[
            models.UniqueConstraint(fields=["id_admin", "enterprise"], name="enterprise_id_admin_subfamily_unique_idx")
        ]

class Brand(models.Model):
    name = models.CharField(max_length=100, unique=False, blank=True)
    id_admin = models.IntegerField()
    enterprise = models.CharField(max_length=100, unique=False, blank=False, default="none")

    def __str__(self):
        return self.name
    
    class Meta:
        db_table = "brand"
        verbose_name = "Brand"
        verbose_name_plural = "Brands"
        constraints=[
            models.UniqueConstraint(fields=["id_admin", "enterprise"], name="enterprise_id_admin_brand_unique_idx")
        ]

class Catalog(models.Model):
    name = models.CharField(max_length=100, unique=False, blank=True)   

    def __str__(self):
        return self.name
    
    class Meta:
        db_table = "catalog"
        verbose_name = "Catalog"
        verbose_name_plural = "Catalogs"
        constraints=[
            models.UniqueConstraint(fields=["name"], name="catalog_name_unique_idx") 
        ]

class Product(models.Model):
    family = models.ForeignKey(Family, on_delete=models.PROTECT, related_name='products')
    subfamily = models.ForeignKey(SubFamily, on_delete=models.PROTECT, related_name='products')
    brand = models.ForeignKey(Brand, on_delete=models.PROTECT, related_name='products', null=True, blank=True)
    catalog = models.ForeignKey(Catalog, on_delete=models.PROTECT, related_name='products', null=True, blank=True)
    id_admin = models.IntegerField()
    code = models.CharField(max_length=100, unique=True)
    description = models.TextField()
    create_date = models.DateTimeField()
    enterprise = models.CharField(max_length=100, unique=False, blank=False, default="none")


    def __str__(self):
        return self.description
    
    class Meta:
        db_table = "product"
        verbose_name = "Product"
        verbose_name_plural = "Products"
        constraints=[
            models.UniqueConstraint(fields=["id_admin", "enterprise"], name="enterprise_id_admin_product_unique_idx")
        ]


class ProductABC(models.Model):
    product = models.ForeignKey(Product, on_delete=models.PROTECT, related_name='abc_products')    

    total_amount = models.DecimalField(max_digits=25, decimal_places=5, default=0, null=True, blank=True)
    profit = models.DecimalField(max_digits=25, decimal_places=5, default=0, null=True, blank=True)
    profit_percentage = models.DecimalField(max_digits=25, decimal_places=5, default=0, null=True, blank=True)
    units_sold = models.IntegerField(default=0, null=True, blank=True)
    inventory_close_u = models.DecimalField(max_digits=25, decimal_places=5, default=0, null=True, blank=True)
    inventory_close_p = models.DecimalField(max_digits=25, decimal_places=5, default=0, null=True, blank=True)
    monthly_roi = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    sold_average_month = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    profit_average_month = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    actual_inventory = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    average_selling_cost = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_average_u = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_average_p = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_days = models.IntegerField(null=True, blank=True)
    sales_percentage = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    acc_sales_percentage = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    sold_abc = models.CharField(max_length=1, unique=False, null=True, blank=True)
    profit_percentage = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    acc_profit_percentage = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    profit_abc = models.CharField(max_length=1, unique=False, null=True, blank=True)
    top_products = models.CharField(max_length=5, unique=False, null=True, blank=True)
    month_sale_u_january = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_p_january = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_u_january = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_p_january = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_u_february = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_p_february = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_u_february = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_p_february = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_u_march = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_p_march = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_u_march = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_p_march = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_u_april = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_p_april = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_u_april = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_p_april = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_u_may = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_p_may = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_u_may = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_p_may = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_u_june = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_p_june = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_u_june = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_p_june = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_u_july = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_p_july = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_u_july = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_p_july = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_u_august = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_p_august = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_u_august = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_p_august = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_u_september = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_p_september = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_u_september = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_p_september = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_u_october = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_p_october = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_u_october = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_p_october = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_u_november = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_p_november = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_u_november = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_p_november = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_u_december = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_p_december = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_u_december = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_p_december = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)

    enterprise = models.CharField(max_length=100)
    year = models.IntegerField()
    last_update = models.DateTimeField(blank=True, null=True)

    class Meta:
        db_table = "product_abc"
        verbose_name = "Product ABC"
        verbose_name_plural = "Products ABC"

class AnalysisABC(models.Model):
    catalog = models.ForeignKey(Catalog, on_delete=models.PROTECT, related_name='abc_analysis')

    total_amount = models.DecimalField(max_digits=25, decimal_places=5, default=0, null=True, blank=True)
    profit = models.DecimalField(max_digits=25, decimal_places=5, default=0, null=True, blank=True)
    profit_percentage = models.DecimalField(max_digits=25, decimal_places=5, default=0, null=True, blank=True)
    units_sold = models.IntegerField(default=0, null=True, blank=True)
    inventory_close_u = models.DecimalField(max_digits=25, decimal_places=5, default=0, null=True, blank=True)
    inventory_close_p = models.DecimalField(max_digits=25, decimal_places=5, default=0, null=True, blank=True)
    monthly_roi = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    sold_average_month = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    profit_average_month = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    actual_inventory = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    average_selling_cost = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_average_u = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_average_p = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_days = models.IntegerField(null=True, blank=True)
    sales_percentage = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    acc_sales_percentage = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    sold_abc = models.CharField(max_length=1, unique=False, null=True, blank=True)
    profit_percentage = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    acc_profit_percentage = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    profit_abc = models.CharField(max_length=1, unique=False, null=True, blank=True)
    top_products = models.CharField(max_length=5, unique=False, null=True, blank=True)
    month_sale_u_january = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_p_january = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_u_january = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_p_january = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_u_february = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_p_february = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_u_february = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_p_february = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_u_march = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_p_march = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_u_march = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_p_march = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_u_april = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_p_april = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_u_april = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_p_april = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_u_may = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_p_may = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_u_may = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_p_may = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_u_june = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_p_june = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_u_june = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_p_june = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_u_july = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_p_july = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_u_july = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_p_july = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_u_august = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_p_august = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_u_august = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_p_august = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_u_september = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_p_september = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_u_september = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_p_september = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_u_october = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_p_october = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_u_october = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_p_october = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_u_november = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_p_november = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_u_november = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_p_november = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_u_december = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    month_sale_p_december = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_u_december = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)
    inventory_close_p_december = models.DecimalField(max_digits=25, decimal_places=5, null=True, blank=True)

    enterprise = models.CharField(max_length=100)
    year = models.IntegerField()
    last_update = models.DateTimeField(blank=True, null=True)

    class Meta:
        db_table = "analysis_abc"
        verbose_name = "Analysis ABC"
        verbose_name_plural = "Analysis ABCs"

class Movements(models.Model):
    id_admin = models.IntegerField()
    movement_date = models.DateTimeField()
    is_order = models.BooleanField(default=False)
    is_input = models.BooleanField(default=False)
    is_output = models.BooleanField(default=False)
    pending = models.BooleanField(default=False)
    folio = models.IntegerField()
    serie = models.CharField(max_length=250, blank=True, null=True)
    aditional_folio = models.CharField(max_length=250, blank=True, null=True)
    paid = models.BooleanField(default=False)
    payment_method = models.CharField(max_length=250)
    quantity = models.DecimalField(max_digits=25, decimal_places=5, default=0, null=True, blank=True)
    total_quantity = models.DecimalField(max_digits=25, decimal_places=5, default=0, null=True, blank=True)
    amount = models.DecimalField(max_digits=25, decimal_places=5, default=0, null=True, blank=True)
    iva = models.DecimalField(max_digits=25, decimal_places=5, default=0, null=True, blank=True)
    discount = models.DecimalField(max_digits=25, decimal_places=5, default=0, null=True, blank=True)
    amount_discount = models.DecimalField(max_digits=25, decimal_places=5, default=0, null=True, blank=True)
    total = models.DecimalField(max_digits=25, decimal_places=5, default=0, null=True, blank=True)
    cost_of_sale = models.DecimalField(max_digits=25, decimal_places=5, default=0, null=True, blank=True)
    profit = models.DecimalField(max_digits=25, decimal_places=5, default=0, null=True, blank=True)
    currency = models.CharField(max_length=100, unique=False, blank=True)
    order_id_admin = models.IntegerField(null=True, blank=True)
    movement_type = models.IntegerField()
    canceled = models.BooleanField(default=False)
    enterprise = models.CharField(max_length=100)

    class Meta:
        db_table = "movements"
        verbose_name = "Movement"
        verbose_name_plural = "Movements"
        constraints = [
            models.UniqueConstraint(fields=['id_admin', 'enterprise'], name='enterprise_id_admin_unique')
        ]


class MovementsDetail(models.Model):
    id_admin = models.IntegerField()
    movement = models.ForeignKey(Movements, on_delete=models.PROTECT, related_name='details')
    product = models.ForeignKey(Product, on_delete=models.PROTECT, related_name='movement_details', null=True, blank=True)
    movement_detail_date = models.DateTimeField()
    um = models.CharField(max_length=100, unique=False, blank=True)
    quantity = models.DecimalField(max_digits=25, decimal_places=5, default=0, null=True, blank=True)
    um_factor = models.DecimalField(max_digits=25, decimal_places=5, default=0, null=True, blank=True)
    unitary_price = models.DecimalField(max_digits=25, decimal_places=5, default=0, null=True, blank=True)
    total_quantity = models.DecimalField(max_digits=25, decimal_places=5, default=0, null=True, blank=True)
    amount = models.DecimalField(max_digits=25, decimal_places=5, default=0, null=True, blank=True)
    iva = models.DecimalField(max_digits=25, decimal_places=5, default=0, null=True, blank=True)
    discount = models.DecimalField(max_digits=25, decimal_places=5, default=0, null=True, blank=True)
    existence = models.DecimalField(max_digits=25, decimal_places=5, default=0, null=True, blank=True)
    cost_of_sale = models.DecimalField(max_digits=25, decimal_places=5, default=0, null=True, blank=True)
    canceled = models.BooleanField(default=False)
    enterprise = models.CharField(max_length=100)

    class Meta:
        db_table = "movements_detail"
        verbose_name = "Movement Detail"
        verbose_name_plural = "Movements Details"
