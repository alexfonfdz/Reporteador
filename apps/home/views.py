# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from django import template
from django.contrib.auth.decorators import login_required
from django.http import HttpResponse, HttpResponseBadRequest, HttpResponseNotAllowed, HttpResponseRedirect
from django.template import loader
from django.urls import reverse
from django.http import JsonResponse
from django.core.paginator import Paginator
from datetime import datetime, date
from django.views.decorators.csrf import csrf_exempt
from apps.home.models import ProductABC, AnalysisABC, Family, SubFamily, Brand, Catalog
from core.settings import ENV_PSQL_NAME, ENV_PSQL_USER, ENV_PSQL_PASSWORD, ENV_PSQL_HOST, ENV_PSQL_PORT, ENV_PSQL_DB_SCHEMA, ENV_MYSQL_NAME, ENV_MYSQL_USER, ENV_MYSQL_PASSWORD, ENV_MYSQL_HOST, ENV_MYSQL_PORT, ENV_UPDATE_ALL_DATES
import mysql.connector as m
import json
import asyncio
import psycopg2 as p
import pandas as pd
from .product_abc_logic import enterprises


@login_required(login_url="/login/")
def index(request):
    context = {'segment': 'index'}

    html_template = loader.get_template('home/index.html')
    return HttpResponse(html_template.render(context, request))

@login_required(login_url="/login/")
def pages(request):

    context = {}
    # All resource paths end in .html.
    # Pick out the html file name from the url. And load that template.
    try:

        load_template = request.path.split('/')[-1]

        if load_template == 'admin':
            return HttpResponseRedirect(reverse('admin:index'))
        context['segment'] = load_template

        html_template = loader.get_template('home/' + load_template)
        return HttpResponse(html_template.render(context, request))

    except template.TemplateDoesNotExist:

        html_template = loader.get_template('home/page-404.html')
        return HttpResponse(html_template.render(context, request))

    except:
        html_template = loader.get_template('home/page-500.html')
        return HttpResponse(html_template.render(context, request))


def get_years(request):
    conn = p.connect(dbname= ENV_PSQL_NAME, user=ENV_PSQL_USER, host=ENV_PSQL_HOST, password=ENV_PSQL_PASSWORD, port=ENV_PSQL_PORT)
    cur = conn.cursor()
    cur.execute(f"SELECT TO_CHAR(fecha, 'YYYY') AS año, CAST(SUM(saldo_oc) as money) as suma_de_BO, CAST(SUM(importe)AS MONEY) as Suma_de_importe FROM {ENV_PSQL_DB_SCHEMA}.admintotal_movimientodetalle  GROUP BY año ORDER BY año ASC")
    dataYears  = cur.fetchall()
    cur.close() 
    conn.close()

    return JsonResponse(dataYears, safe = False)

@csrf_exempt
def get_months(request):
    if request.method == "GET":
        # Validar que el parámetro 'year' esté presente en la URL
        year = request.GET.get('year')
        if not year:
            return JsonResponse({'error': 'El parámetro "year" es requerido.'}, status=400)

        try:
            # Validar que el año sea un número
            year = int(year)
        except ValueError:
            return JsonResponse({'error': 'El parámetro "year" debe ser un número válido.'}, status=400)

        # Conexión a la base de datos
        conn = p.connect(
            dbname=ENV_PSQL_NAME,
            user=ENV_PSQL_USER,
            host=ENV_PSQL_HOST,
            password=ENV_PSQL_PASSWORD,
            port=ENV_PSQL_PORT
        )
        cur = conn.cursor()

        # Ejecutar la consulta
        cur.execute(f"""
            SELECT TO_CHAR(fecha, 'MONTH') AS MES, 
                   CAST(SUM(saldo_oc) AS money) AS suma_de_BO, 
                   CAST(SUM(importe) AS MONEY) AS Suma_de_importe 
            FROM {ENV_PSQL_DB_SCHEMA}.admintotal_movimientodetalle 
            WHERE extract(year FROM {ENV_PSQL_DB_SCHEMA}.admintotal_movimientodetalle.fecha) = %s 
            GROUP BY MES 
            ORDER BY MES ASC
        """, [year])
        dataYearMonth = cur.fetchall()

        # Cerrar conexión
        cur.close()
        conn.close()

        # Retornar los datos como JSON
        return JsonResponse(dataYearMonth, safe=False)

    else:
        return JsonResponse({'error': 'Método no permitido. Usa GET.'}, status=405)

@csrf_exempt
def get_days(request):
    data = json.loads(request.body)
    year = data['year']
    month = data['month']
    month = month.strip()
    month = datetime.strptime(month, "%B").strftime("%m")
    conn=p.connect(dbname= ENV_PSQL_NAME, user=ENV_PSQL_USER, host=ENV_PSQL_HOST, password=ENV_PSQL_PASSWORD, port=ENV_PSQL_PORT)
    cur = conn.cursor()
    cur.execute(f"SELECT TO_CHAR(fecha, 'DD-MM-YYYY') AS DIA, CAST(SUM(saldo_oc) as money) as suma_de_BO, CAST(SUM(importe)AS MONEY) as Suma_de_importe FROM {ENV_PSQL_DB_SCHEMA}.admintotal_movimientodetalle WHERE extract(year from {ENV_PSQL_DB_SCHEMA}.admintotal_movimientodetalle.fecha) = {year} AND extract(MONTH from {ENV_PSQL_DB_SCHEMA}.admintotal_movimientodetalle.fecha) = {month}  GROUP BY DIA ORDER BY DIA ASC")             
    dataDay  = cur.fetchall()
    cur.close()
    conn.close()

    return JsonResponse(dataDay, safe = False)

@csrf_exempt
def get_products(request):
    data = json.loads(request.body)
    day = data['day']
    conn=p.connect(dbname= ENV_PSQL_NAME, user=ENV_PSQL_USER, host=ENV_PSQL_HOST, password=ENV_PSQL_PASSWORD, port=ENV_PSQL_PORT)
    cur = conn.cursor()
    cur.execute(f"SELECT admintotal_producto.descripcion, CAST(SUM(saldo_oc) as money) as suma_de_BO, CAST(SUM(importe)AS MONEY) as Suma_de_importe FROM {ENV_PSQL_DB_SCHEMA}.admintotal_producto INNER JOIN {ENV_PSQL_DB_SCHEMA}.admintotal_movimientodetalle ON {ENV_PSQL_DB_SCHEMA}.admintotal_producto.id = admintotal_movimientodetalle.producto_id WHERE DATE({ENV_PSQL_DB_SCHEMA}.admintotal_movimientodetalle.fecha) = TO_DATE('{day}', 'DD-MM-YYYY') GROUP BY admintotal_producto.descripcion")          
    dataProducts = cur.fetchall()
    cur.close()
    conn.close()

    return JsonResponse(dataProducts, safe = False)

@csrf_exempt
def get_folio(request):
    data = json.loads(request.body)
    day = data['day']
    product = data['product']
    conn = p.connect(dbname= ENV_PSQL_NAME, user=ENV_PSQL_USER, host=ENV_PSQL_HOST, password=ENV_PSQL_PASSWORD, port=ENV_PSQL_PORT)
    cur = conn.cursor()
    cur.execute(f"SELECT admintotal_movimiento.folio, CAST(SUM(admintotal_movimientodetalle.saldo_oc) as money) as suma_de_BO, CAST(SUM(admintotal_movimientodetalle.importe) AS MONEY) as Suma_de_importe FROM {ENV_PSQL_DB_SCHEMA}.admintotal_movimiento INNER JOIN {ENV_PSQL_DB_SCHEMA}.admintotal_movimientodetalle ON {ENV_PSQL_DB_SCHEMA}.admintotal_movimiento.poliza_ptr_id = {ENV_PSQL_DB_SCHEMA}.admintotal_movimientodetalle.movimiento_id INNER JOIN {ENV_PSQL_DB_SCHEMA}.admintotal_producto ON {ENV_PSQL_DB_SCHEMA}.admintotal_movimientodetalle.producto_id = {ENV_PSQL_DB_SCHEMA}.admintotal_producto.id WHERE DATE({ENV_PSQL_DB_SCHEMA}.admintotal_movimientodetalle.fecha) = TO_DATE('{day}', 'DD-MM-YYYY') AND {ENV_PSQL_DB_SCHEMA}.admintotal_producto.descripcion = '{product}' GROUP BY admintotal_movimiento.folio")
    dataProductsFolio  = cur.fetchall()
    cur.close()
    conn.close()

    return JsonResponse(dataProductsFolio, safe = False)

@csrf_exempt
def get_clientes_nuevos(request):
    filtro_fecha = request.GET.get('date-control', 'un_mes')
    descripcion = request.GET.get('descripcion', '')

    if filtro_fecha == 'un_mes':
        intervalo = '1 MONTH'
    elif filtro_fecha == 'tres_meses':
        intervalo = '3 MONTHS'
    elif filtro_fecha == 'seis_meses':
        intervalo = '6 MONTHS'
    else:
        intervalo = '1 MONTH'  # Default

    conn = p.connect(dbname= ENV_PSQL_NAME, user=ENV_PSQL_USER, host=ENV_PSQL_HOST, password=ENV_PSQL_PASSWORD, port=ENV_PSQL_PORT)
    cur = conn.cursor()
    cur.execute(f"""
            SELECT c.id, c.razon_social AS nombre, c.rfc, c.creado as fecha_registro
            FROM {ENV_PSQL_DB_SCHEMA}.admintotal_cliente c
            WHERE c.creado >= NOW() - INTERVAL '{intervalo}'
            AND c.razon_social ILIKE %s
        """, [f'%{descripcion}%'])
    clientes = cur.fetchall()
    cur.close()
    conn.close()

    paginator = Paginator(clientes, 10)  # Mostrar 10 registros por página
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    data = {
        'clientes': list(page_obj),
        'has_next': page_obj.has_next(),
        'has_previous': page_obj.has_previous(),
        'num_pages': paginator.num_pages,
        'current_page': page_obj.number,
    }

    return JsonResponse(data, safe=False)

@csrf_exempt
def get_clientes_ausentes(request):
    filtro_fecha = request.GET.get('date-control', 'un_mes')
    descripcion = request.GET.get('descripcion', '')

    if filtro_fecha == 'un_mes':
        intervalo = '1 MONTH'
    elif filtro_fecha == 'dos_meses':
        intervalo = '2 MONTHS'
    elif filtro_fecha == 'tres_meses':
        intervalo = '3 MONTHS'
    elif filtro_fecha == 'seis_meses':
        intervalo = '6 MONTHS'
    else:
        intervalo = '1 MONTH'  # Default

    conn = p.connect(dbname= ENV_PSQL_NAME, user=ENV_PSQL_USER, host=ENV_PSQL_HOST, password=ENV_PSQL_PASSWORD, port=ENV_PSQL_PORT)
    cur = conn.cursor()
    cur.execute(f"""
            SELECT c.id, c.razon_social AS nombre, c.rfc, fecha_ultima_venta
	        FROM {ENV_PSQL_DB_SCHEMA}.admintotal_cliente c 
            WHERE c.fecha_ultima_venta  < NOW() - INTERVAL '{intervalo}'
            AND c.razon_social ILIKE %s
        """, [f'%{descripcion}%'])
    clientes = cur.fetchall()
    cur.close()
    conn.close()

    paginator = Paginator(clientes, 10)  # Mostrar 10 registros por página
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    data = {
        'clientes': list(page_obj),
        'has_next': page_obj.has_next(),
        'has_previous': page_obj.has_previous(),
        'num_pages': paginator.num_pages,
        'current_page': page_obj.number,
    }

    return JsonResponse(data, safe=False)

@csrf_exempt
def get_almacenes(request):
    conn = p.connect(dbname= ENV_PSQL_NAME, user=ENV_PSQL_USER, host=ENV_PSQL_HOST, password=ENV_PSQL_PASSWORD, port=ENV_PSQL_PORT)
    cur = conn.cursor()
    cur.execute(f"SELECT id, nombre FROM {ENV_PSQL_DB_SCHEMA}.admintotal_almacen")
    almacenes = cur.fetchall()
    cur.close()
    conn.close()

    return JsonResponse(almacenes, safe=False)

@csrf_exempt
def get_almacen_data(request):
    almacen_id = request.GET.get('almacen_id')
    page_number = request.GET.get('page', 1)
    items_per_page = 10  # Número de elementos por página

    conn = p.connect(dbname=ENV_PSQL_NAME, user=ENV_PSQL_USER, host=ENV_PSQL_HOST, password=ENV_PSQL_PASSWORD, port=ENV_PSQL_PORT)
    cur = conn.cursor()
    cur.execute(f"""
        SELECT
            P.CODIGO,
            ROUND(PA.DISPONIBLE, 0) AS DISPONIBLE,
            ROUND((SUM(MD.SALDO_VENDER_PEDIDO) / 12) * 2, 0) AS INV_SUG,
            ROUND(SUM(MD.SALDO_VENDER_PEDIDO) / 12, 2) AS PROM_VENTA,
            CASE 
                WHEN ROUND((SUM(MD.SALDO_VENDER_PEDIDO) / 12)) = 0 THEN 0.00
                ELSE ROUND(PA.DISPONIBLE / ROUND((SUM(MD.SALDO_VENDER_PEDIDO) / 12)),2)
            END AS mi,
            ROUND(P.PESO) AS PESO
        FROM
            {ENV_PSQL_DB_SCHEMA}.ADMINTOTAL_PRODUCTOALMACEN PA
            INNER JOIN {ENV_PSQL_DB_SCHEMA}.ADMINTOTAL_PRODUCTO P ON P.ID = PA.PRODUCTO_ID
            INNER JOIN {ENV_PSQL_DB_SCHEMA}.ADMINTOTAL_ALMACEN A ON A.ID = PA.ALMACEN_ID
            INNER JOIN {ENV_PSQL_DB_SCHEMA}.ADMINTOTAL_MOVIMIENTODETALLE MD ON MD.PXA_ID = PA.ID
        WHERE
            PA.ALMACEN_ID = %s
            AND MD.CANCELADO = FALSE
            AND MD.FECHA BETWEEN CURRENT_DATE - INTERVAL '1 year' AND CURRENT_DATE
        GROUP BY
            P.CODIGO,
            PA.DISPONIBLE,
            P.PESO
    """, [almacen_id])
    almacen_data = cur.fetchall()
    cur.close()
    conn.close()

    paginator = Paginator(almacen_data, items_per_page)
    page_obj = paginator.get_page(page_number)

    data = {
        'almacen_data': list(page_obj),
        'has_next': page_obj.has_next(),
        'has_previous': page_obj.has_previous(),
        'num_pages': paginator.num_pages,
        'current_page': page_obj.number,
    }

    return JsonResponse(data, safe=False)

########################## AQUÍ INICIAN LOS ENDPOINTS PARA TODO LO DE PRODUCT_ABC Y ANÁLISIS_ABC ##########################
# ==================================================================================================== # 
df = pd.read_csv('mock.csv')
df = df.where(pd.notnull(df), None)
records = df.to_dict(orient='records')
@csrf_exempt
def debug_productosabc(request):
    if request.method != 'GET':
        return HttpResponseNotAllowed(['Get'])

    paginator = Paginator(records, 10)
    page = paginator.page(request.GET.get('page',1))

    page_content = list(page)

    data ={
        "data": page_content,
        "has_next": page.has_next(),
        "has_previous": page.has_previous(),
        "num_pages": paginator.num_pages,
        "current_page": page.number
    }

    
    return JsonResponse(data, safe=False)

@csrf_exempt
def get_enterprises(request):
    return JsonResponse(list(enterprises.keys()), safe=False)


@csrf_exempt
def get_products_abc(request):
    if request.method != 'GET':
        return HttpResponseNotAllowed(['Get'])

    # Obtener filtros desde searchParams
    family = request.GET.get('family', '').strip()
    subfamily = request.GET.get('subfamily', '').strip()
    brand = request.GET.get('brand', '').strip()
    catalog = request.GET.get('catalog', '').strip()
    year_start = request.GET.get('year_start', '').strip()
    year_end = request.GET.get('year_end', '').strip()
    enterprise_key = request.GET.get('enterprise', '').strip() 

    # Determinar el schema a partir del enterprise_key
    enterprise_data = enterprises.get(enterprise_key)
    schema = enterprise_data.schema if enterprise_data else ''

    # Construir el queryset con filtros
    qs = ProductABC.objects.select_related(
        'product',
        'product__family',
        'product__subfamily',
        'product__brand',
        'product__catalog'
    )

    if family:
        qs = qs.filter(product__family__name__icontains=family)
    if subfamily:
        qs = qs.filter(product__subfamily__name__icontains=subfamily)
    if brand:
        qs = qs.filter(product__brand__name__icontains=brand)
    if catalog:
        qs = qs.filter(product__catalog__name__icontains=catalog)
    # Filtro por rango de años
    if year_start and year_end:
        try:
            qs = qs.filter(year__gte=int(year_start), year__lte=int(year_end))
        except ValueError:
            pass
    elif year_start:
        try:
            qs = qs.filter(year__gte=int(year_start))
        except ValueError:
            pass
    elif year_end:
        try:
            qs = qs.filter(year__lte=int(year_end))
        except ValueError:
            pass
    if schema:
        qs = qs.filter(enterprise=schema)

    all_products = list(qs.all())

    page = int(request.GET.get('page', 1))
    per_page = int(request.GET.get('per_page', 10))

    paginator = Paginator(
        object_list=all_products,
        per_page=per_page
    )

    page_obj = paginator.get_page(page)
    data = []
    for abc in page_obj.object_list:
        product = abc.product
        data.append({
            "id": abc.id,
            "sales_percentage": float(abc.sales_percentage) if abc.sales_percentage is not None else None,
            "acc_sales_percentage": float(abc.acc_sales_percentage) if abc.acc_sales_percentage is not None else None,
            "sold_abc": abc.sold_abc,
            "profit_percentage": float(abc.profit_percentage) if abc.profit_percentage is not None else None,
            "acc_profit_percentage": float(abc.acc_profit_percentage) if abc.acc_profit_percentage is not None else None,
            "profit_abc": abc.profit_abc,
            "top_products": abc.top_products,
            "enterprise": abc.enterprise,
            "year": abc.year,
            "last_update": abc.last_update.isoformat() if abc.last_update else None,
            # Información del producto
            "product_id": product.id,
            "product_code": product.code,
            "product_description": product.description,
            # Información de la familia
            "family_name": product.family.name if product.family else None,
            # Información de la subfamilia
            "subfamily_name": product.subfamily.name if product.subfamily else None,
            # Información de la marca
            "brand_name": product.brand.name if product.brand else None,
            # Información del catálogo
            "catalog_name": product.catalog.name if product.catalog else None,
        })

    pagination_data = {
        "num_pages": paginator.num_pages,
        "page": page_obj.number,
    }


    return JsonResponse({
        "data": data,
        "pagination": pagination_data
    })

@csrf_exempt
def get_analysis_abc(request):
    if request.method != 'GET':
        return HttpResponseNotAllowed(['Get'])

    # Obtener filtros desde searchParams
    family = request.GET.get('family', '').strip()
    subfamily = request.GET.get('subfamily', '').strip()
    year_start = request.GET.get('year_start', '').strip()
    year_end = request.GET.get('year_end', '').strip()
    enterprise_key = request.GET.get('enterprise', '').strip()

    # Determinar el schema a partir del enterprise_key
    enterprise_data = enterprises.get(enterprise_key)
    schema = enterprise_data.schema if enterprise_data else ''

    # select_related solo con campos válidos
    qs = AnalysisABC.objects.select_related(
        'family',
        'subfamily',
        'catalog'
    )

    if family:
        qs = qs.filter(family__name__icontains=family)
    if subfamily:
        qs = qs.filter(subfamily__name__icontains=subfamily)
    # Filtro por rango de años
    if year_start and year_end:
        try:
            qs = qs.filter(year__gte=int(year_start), year__lte=int(year_end))
        except ValueError:
            pass
    elif year_start:
        try:
            qs = qs.filter(year__gte=int(year_start))
        except ValueError:
            pass
    elif year_end:
        try:
            qs = qs.filter(year__lte=int(year_end))
        except ValueError:
            pass
    if schema:
        qs = qs.filter(enterprise=schema)

    qs.order_by('total_amount')

    all_analysis = list(qs.all())
    
    page = int(request.GET.get('page', 1))
    per_page = int(request.GET.get('per_page', 10))

    paginator = Paginator(
        object_list=all_analysis,
        per_page=per_page
    )

    page_obj = paginator.get_page(page)
    data = []
    for analysis in page_obj.object_list:
        data.append({
            "id": analysis.id,
            "total_amount": float(analysis.total_amount) if analysis.total_amount is not None else None,
            "profit": float(analysis.profit) if analysis.profit is not None else None,
            "profit_percentage": float(analysis.profit_percentage) if analysis.profit_percentage is not None else None,
            "units_sold": analysis.units_sold,
            "inventory_close_u": float(analysis.inventory_close_u) if analysis.inventory_close_u is not None else None,
            "inventory_close_p": float(analysis.inventory_close_p) if analysis.inventory_close_p is not None else None,
            "monthly_roi": float(analysis.monthly_roi) if analysis.monthly_roi is not None else None,
            "sold_average_month": float(analysis.sold_average_month) if analysis.sold_average_month is not None else None,
            "profit_average_month": float(analysis.profit_average_month) if analysis.profit_average_month is not None else None,
            "actual_inventory": float(analysis.actual_inventory) if analysis.actual_inventory is not None else None,
            "average_selling_cost": float(analysis.average_selling_cost) if analysis.average_selling_cost is not None else None,
            "inventory_average_u": float(analysis.inventory_average_u) if analysis.inventory_average_u is not None else None,
            "inventory_average_p": float(analysis.inventory_average_p) if analysis.inventory_average_p is not None else None,
            "inventory_days": analysis.inventory_days,
            "sales_percentage": float(analysis.sales_percentage) if analysis.sales_percentage is not None else None,
            "acc_sales_percentage": float(analysis.acc_sales_percentage) if analysis.acc_sales_percentage is not None else None,
            "sold_abc": analysis.sold_abc,
            "profit_abc": analysis.profit_abc,
            "acc_profit_percentage": float(analysis.acc_profit_percentage) if analysis.acc_profit_percentage is not None else None,
            "top_products": analysis.top_products,
            "enterprise": analysis.enterprise,
            "year": analysis.year,
            "last_update": analysis.last_update.isoformat() if analysis.last_update else None,
            # Información de la familia
            "family_name": analysis.family.name if analysis.family else None,
            # Información de la subfamilia
            "subfamily_name": analysis.subfamily.name if analysis.subfamily else None,
            # Información del catálogo
            "catalog_name": analysis.catalog.name if analysis.catalog else None,
        })

    pagination_data = {
        "num_pages": paginator.num_pages,
        "page": page_obj.number,
    }

    return JsonResponse({
        "data": data,
        "pagination": pagination_data
    })

@csrf_exempt
def get_families(request):
    if request.method != 'GET':
        return HttpResponseNotAllowed(['GET'])
    enterprise = request.GET.get('enterprise', '').strip()
    qs = Family.objects.all()
    if enterprise:
        enterprise_data = enterprises.get(enterprise)
        if enterprise_data:
            enterprise = enterprise_data.schema
        qs = qs.filter(enterprise=enterprise)
    # Solo nombres únicos
    families = qs.values('name').distinct()
    return JsonResponse(list(families), safe=False)

@csrf_exempt
def get_subfamilies(request):
    if request.method != 'GET':
        return HttpResponseNotAllowed(['GET'])
    enterprise = request.GET.get('enterprise', '').strip()
    qs = SubFamily.objects.all()
    if enterprise:
        enterprise_data = enterprises.get(enterprise)
        if enterprise_data:
            enterprise = enterprise_data.schema
            qs = qs.filter(enterprise=enterprise)
    # Solo nombres únicos
    subfamilies = qs.values('name').distinct()
    return JsonResponse(list(subfamilies), safe=False)

@csrf_exempt
def get_brands(request):
    if request.method != 'GET':
        return HttpResponseNotAllowed(['GET'])
    enterprise = request.GET.get('enterprise', '').strip()
    qs = Brand.objects.all()
    if enterprise:
        enterprise_data = enterprises.get(enterprise)
        if enterprise_data:
            enterprise = enterprise_data.schema
            qs = qs.filter(enterprise=enterprise)
    brands = qs.values('name').distinct()
    return JsonResponse(list(brands), safe=False)

@csrf_exempt
def get_catalogs(request):
    if request.method != 'GET':
        return HttpResponseNotAllowed(['GET'])
    enterprise = request.GET.get('enterprise', '').strip()
    qs = Catalog.objects.all()
    # Catalogs may not have enterprise, but keep for symmetry
    # If you want to filter by enterprise, add logic here
    catalogs = qs.values('name').distinct()
    return JsonResponse(list(catalogs), safe=False)
