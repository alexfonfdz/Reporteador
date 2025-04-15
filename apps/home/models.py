# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from django.db import models
from django.contrib.auth.models import User

# Create your models here.

class CA_Productos(models.Model):
    id = models.BigAutoField(primary_key=True)
    codigo = models.CharField(max_length=50)
    descripcion = models.CharField(max_length=255)
    marca = models.CharField(max_length=100)
    linea_top = models.CharField(max_length=100)
    voltaje = models.CharField(max_length=50)
    doble_relacion = models.CharField(max_length=50)
    familia = models.CharField(max_length=100)
    sub_familia = models.CharField(max_length=100)
    tipo4 = models.CharField(max_length=100)
    marca5 = models.CharField(max_length=100)
    sublinea = models.CharField(max_length=100)

    def __str__(self):
        return self.descripcion
