# Generated by Django 3.2.6 on 2025-06-23 08:12

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('home', '0012_report_configuration'),
    ]

    operations = [
        migrations.AddConstraint(
            model_name='product',
            constraint=models.UniqueConstraint(fields=('code', 'enterprise'), name='code_enterprise_unique_idx'),
        ),
    ]
