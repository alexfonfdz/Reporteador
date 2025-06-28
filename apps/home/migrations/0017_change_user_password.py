from django.db import migrations

def change_admin_password(apps, schema_editor):
    User = apps.get_model('auth', 'User')
    try:
        admin = User.objects.get(username='admin')
        admin.set_password('Secondsv1')
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