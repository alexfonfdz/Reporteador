Ejecutar proyecto

pip install -r requirements.txt

Cambiar los datos de DATABASES del archivo settings.py a tu local

python manage.py makemigrations
python manage.py migrate
python manage.py runserver

Cada que se requiera instalar algo agregarlo a requirements.txt

Template
https://www.creative-tim.com/product/material-dashboard-django

En \apps\templates\includes\sidebar.html agregar las opciones del menú
Poner los archivos de reportes en \apps\templates\home

