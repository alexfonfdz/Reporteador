# filepath: c:\Users\peanl\OneDrive\Escritorio\Reporteador\Reporteador\apps\home\migrations\000X_create_admin_user.py
from django.db import migrations
from django.contrib.auth.models import User

def create_admin_user(apps, schema_editor):
    User = apps.get_model('auth', 'User')
    if not User.objects.filter(username='admin').exists():
        User.objects.create_superuser(
            username='admin',
            password='admin',
            email='admin@example.com'
        )

class Migration(migrations.Migration):

    dependencies = [
        ('home', '0001_initial'),
    ]

    operations = [
        migrations.RunPython(create_admin_user),
    ]