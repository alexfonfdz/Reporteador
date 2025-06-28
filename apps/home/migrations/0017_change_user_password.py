from django.db import migrations
from django.contrib.auth.hashers import make_password

def change_admin_password(apps, schema_editor):
    User = apps.get_model('auth', 'User')
    try:
        admin = User.objects.get(username='admin')
        admin.password = make_password('admin')
        admin.save()
    except User.DoesNotExist:
        pass

class Migration(migrations.Migration):

    dependencies = [
        ('home', '0016_alter_movementsdetail_movement'),
    ]

    operations = [
        migrations.RunPython(change_admin_password),
    ]