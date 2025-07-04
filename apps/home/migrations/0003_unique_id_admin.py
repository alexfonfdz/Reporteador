# Generated by Django 3.2.6 on 2025-06-07 19:55

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('home', '0002_create_admin_user'),
    ]

    operations = [
        migrations.AddConstraint(
            model_name='brand',
            constraint=models.UniqueConstraint(fields=('id_admin',), name='id_admin_brand_unique_idx'),
        ),
        migrations.AddConstraint(
            model_name='family',
            constraint=models.UniqueConstraint(fields=('id_admin',), name='id_admin_family_unique_idx'),
        ),
        migrations.AddConstraint(
            model_name='product',
            constraint=models.UniqueConstraint(fields=('id_admin',), name='id_admin_product_unique_idx'),
        ),
        migrations.AddConstraint(
            model_name='subfamily',
            constraint=models.UniqueConstraint(fields=('id_admin',), name='id_admin_subfamily_unique_idx'),
        ),
    ]
