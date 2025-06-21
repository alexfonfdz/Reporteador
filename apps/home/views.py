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
from apps.home.models import ProductABC, AnalysisABC, Family, SubFamily, Brand, Catalog, Movements, Product
from core.settings import ENV_PSQL_NAME, ENV_PSQL_USER, ENV_PSQL_PASSWORD, ENV_PSQL_HOST, ENV_PSQL_PORT, ENV_PSQL_DB_SCHEMA, ENV_MYSQL_NAME, ENV_MYSQL_USER, ENV_MYSQL_PASSWORD, ENV_MYSQL_HOST, ENV_MYSQL_PORT, ENV_UPDATE_ALL_DATES
import mysql.connector as m
import json
import asyncio
import psycopg2 as p
import pandas as pd
import aiomysql
import os
from django.conf import settings
from .product_abc_logic import enterprises
from apps.home.queries.mysql import GET_PRODUCTS_SUMMARY_BY_RANGE
from django.db.models import Min
from django.db.models.functions import ExtractYear


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
        return HttpResponseNotAllowed(['GET'])

    # Obtener filtros desde searchParams
    family = request.GET.get('family', '').strip()
    subfamily = request.GET.get('subfamily', '').strip()
    brand = request.GET.get('brand', '').strip()
    catalog = request.GET.get('catalog', '').strip()
    date_start = request.GET.get('date_start', '').strip()
    date_end = request.GET.get('date_end', '').strip()
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
    if schema:
        qs = qs.filter(enterprise=schema)

    # Obtener los productos filtrados
    all_products = list(qs.all())

    # Validar fecha mínima
    min_date = Movements.objects.aggregate(min_date=Min('movement_date'))['min_date']
    if not date_start or (min_date and date_start < min_date.strftime('%Y-%m-%d')):
        return JsonResponse({"error": "La fecha de inicio debe ser igual o posterior a la fecha mínima de movimientos: %s" % (min_date.strftime('%Y-%m-%d') if min_date else '-')}, status=400)

    # Ajustar fecha final
    date_start_val = date_start
    date_end_val = None
    if date_end:
        date_end_val = date_end + " 23:59:59"

    # Obtener años en el rango
    years = []
    if date_start and date_end:
        try:
            y_start = int(date_start[:4])
            y_end = int(date_end[:4])
            years = list(range(y_start, y_end + 1))
        except Exception:
            years = []
    elif date_start:
        try:
            y_start = int(date_start[:4])
            years = [y_start]
        except Exception:
            years = []

    # Mapear product_id a sus totales por rango
    async def fetch_totals():
        results = {}
        if not (date_start_val and date_end_val):
            return results
        conn = await aiomysql.connect(
            host=ENV_MYSQL_HOST,
            port=int(ENV_MYSQL_PORT),
            user=ENV_MYSQL_USER,
            password=ENV_MYSQL_PASSWORD,
            db=ENV_MYSQL_NAME,
            autocommit=True,
        )
        try:
            async with conn.cursor() as cur:
                for abc in all_products:
                    product = abc.product
                    family_id = product.family.id if product.family else None
                    subfamily_id = product.subfamily.id if product.subfamily else None
                    catalog_id = product.catalog.id if product.catalog else None
                    if not (catalog_id and family_id and subfamily_id):
                        continue
                    await cur.execute(
                        GET_PRODUCTS_SUMMARY_BY_RANGE(schema),
                        (catalog_id, family_id, subfamily_id, date_start_val, date_end_val)
                    )
                    row = await cur.fetchone()
                    if row:
                        results[abc.id] = {
                            "total_amount": float(row[0]) if row[0] is not None else 0,
                            "profit": float(row[1]) if row[1] is not None else 0,
                            "units_sold": float(row[2]) if row[2] is not None else 0,
                        }
        finally:
            conn.close()
        return results

    # Recopilar datos ABC por año para stats
    def get_abc_stats_by_year(product_abc, years):
        stats = {}
        for y in years:
            try:
                abc = ProductABC.objects.filter(
                    product=product_abc.product,
                    enterprise=product_abc.enterprise,
                    year=int(y)
                ).first()
                if abc:
                    stats[str(y)] = {
                        "sales_percentage": float(abc.sales_percentage) if abc.sales_percentage is not None else None,
                        "acc_sales_percentage": float(abc.acc_sales_percentage) if abc.acc_sales_percentage is not None else None,
                        "sold_abc": abc.sold_abc,
                        "profit_percentage": float(abc.profit_percentage) if abc.profit_percentage is not None else None,
                        "acc_profit_percentage": float(abc.acc_profit_percentage) if abc.acc_profit_percentage is not None else None,
                        "profit_abc": abc.profit_abc,
                        "top_products": abc.top_products,
                    }
            except Exception as e:
                continue
        return stats

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    totals_map = loop.run_until_complete(fetch_totals())
    loop.close()

    data = []
    for abc in all_products:
        product = abc.product
        totals = totals_map.get(abc.id, {"total_amount": 0, "profit": 0, "units_sold": 0})
        base = {
            "id": abc.id,
            "enterprise": abc.enterprise,
            "product_id": product.id,
            "product_code": product.code,
            "product_description": product.description,
            "family_name": product.family.name if product.family else None,
            "subfamily_name": product.subfamily.name if product.subfamily else None,
            "brand_name": product.brand.name if product.brand else None,
            "catalog_name": product.catalog.name if product.catalog else None,
            "total_amount": totals["total_amount"],
            "profit": totals["profit"],
            "units_sold": totals["units_sold"],
            "year": abc.year,
            "last_update": abc.last_update.isoformat() if abc.last_update else None,
        }
        # ...existing code for stats...
        base["stats"] = get_abc_stats_by_year(abc, years)
        data.append(base)

    # Ordenar primero por total_amount descendente, luego por product_description ascendente
    data.sort(key=lambda x: (-x.get("total_amount", 0), (x.get("product_description") or "").lower()))

    page = int(request.GET.get('page', 1))
    per_page = int(request.GET.get('per_page', 10))
    total_items = len(data)
    num_pages = (total_items + per_page - 1) // per_page
    start = (page - 1) * per_page
    end = start + per_page
    page_data = data[start:end]

    pagination_data = {
        "num_pages": num_pages,
        "page": page,
    }

    return JsonResponse({
        "data": page_data,
        "pagination": pagination_data
    })

@csrf_exempt
def get_analysis_abc(request):
    if request.method != 'GET':
        return HttpResponseNotAllowed(['Get'])

    # Obtener filtros desde searchParams
    family = request.GET.get('family', '').strip()
    subfamily = request.GET.get('subfamily', '').strip()
    year = request.GET.get('year', '').strip()
    top_product = request.GET.get('top_product', '').strip()
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
    if top_product:
        qs = qs.filter(top_products__icontains=top_product) 
    if year:
        try:
            qs = qs.filter(year=int(year))
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
def get_product_top(request):
    if request.method != 'GET':
        return HttpResponseNotAllowed(['GET'])

    enterprise = request.GET.get('enterprise', '').strip()
    year = request.GET.get('year', '').strip()
    family = request.GET.get('family', '').strip()
    subfamily = request.GET.get('subfamily', '').strip()

    
    qs = ProductABC.objects.select_related('product__family', 'product__subfamily')

    if enterprise:
        qs = qs.filter(enterprise=enterprise)

    if year:
        qs = qs.filter(year=year)

    if family:
        qs = qs.filter(product__family__name__icontains=family)

    if subfamily:
        qs = qs.filter(product__subfamily__name__icontains=subfamily)

    top_products_result = qs.values('top_products').distinct()
    top_products = []

    for qs_result in top_products_result.iterator():
        top_product_value = qs_result['top_products']
        if top_product_value:
            top_products.append(top_product_value)

    return JsonResponse(list(top_products), safe=False)



@csrf_exempt
def get_families(request):
    if request.method != 'GET':
        return HttpResponseNotAllowed(['GET'])
    enterprise = request.GET.get('enterprise', '').strip()

    qs = Product.objects.select_related('family')
    if enterprise:
        qs.filter(enterprise=enterprise)

    families = qs.values('family__name').distinct()
    families_names = set()
    
    for qs_result in families.iterator():
        name = qs_result['family__name']
        if name: 
            families_names.add(name)
    
    
    return JsonResponse(list(families_names), safe=False)

@csrf_exempt
def get_subfamilies(request):
    if request.method != 'GET':
        return HttpResponseNotAllowed(['GET'])
    family = request.GET.get('family', '').strip()
    enterprise = request.GET.get('enterprise', '').strip()


    #Get all the     
    qs = Product.objects.select_related('family', 'subfamily')
    if enterprise:
        enterprise_data = enterprises.get(enterprise)
        if enterprise_data:
            enterprise = enterprise_data.schema
            qs = qs.filter(enterprise=enterprise)
    
    if family:
        qs = qs.filter(family__name__icontains=family)

    # Solo nombres únicos de subfamilia
    subfamilies = qs.values('subfamily__name').distinct()

    subfamilies_names = set()
    
    for qs_result in subfamilies.iterator():
        name = qs_result['subfamily__name']
        if name: 
            subfamilies_names.add(name)
    
    return JsonResponse(list(subfamilies_names), safe=False)

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

@csrf_exempt
def get_min_movements_date(request):
    # Devuelve la fecha mínima de Movements
    min_date = None
    try:
        min_date = Movements.objects.aggregate(min_date=Min('movement_date'))['min_date']
    except Exception:
        min_date = None
    return JsonResponse({"min_date": min_date.isoformat() if min_date else None})

@csrf_exempt
def get_products_abc(request):
    if request.method != 'GET':
        return HttpResponseNotAllowed(['GET'])

    # Obtener filtros desde searchParams
    family = request.GET.get('family', '').strip()
    subfamily = request.GET.get('subfamily', '').strip()
    brand = request.GET.get('brand', '').strip()
    catalog = request.GET.get('catalog', '').strip()
    date_start = request.GET.get('date_start', '').strip()
    date_end = request.GET.get('date_end', '').strip()
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
    if schema:
        qs = qs.filter(enterprise=schema)

    # Obtener los productos filtrados
    all_products = list(qs.all())

    # Validar fecha mínima
    min_date = Movements.objects.aggregate(min_date=Min('movement_date'))['min_date']
    if not date_start or (min_date and date_start < min_date.strftime('%Y-%m-%d')):
        return JsonResponse({"error": "La fecha de inicio debe ser igual o posterior a la fecha mínima de movimientos: %s" % (min_date.strftime('%Y-%m-%d') if min_date else '-')}, status=400)

    # Ajustar fecha final
    date_start_val = date_start
    date_end_val = None
    if date_end:
        date_end_val = date_end + " 23:59:59"

    # Obtener años en el rango
    years = []
    if date_start and date_end:
        try:
            y_start = int(date_start[:4])
            y_end = int(date_end[:4])
            years = list(range(y_start, y_end + 1))
        except Exception:
            years = []
    elif date_start:
        try:
            y_start = int(date_start[:4])
            years = [y_start]
        except Exception:
            years = []

    # Mapear product_id a sus totales por rango
    async def fetch_totals():
        results = {}
        if not (date_start_val and date_end_val):
            return results
        conn = await aiomysql.connect(
            host=ENV_MYSQL_HOST,
            port=int(ENV_MYSQL_PORT),
            user=ENV_MYSQL_USER,
            password=ENV_MYSQL_PASSWORD,
            db=ENV_MYSQL_NAME,
            autocommit=True,
        )
        try:
            async with conn.cursor() as cur:
                for abc in all_products:
                    product = abc.product
                    family_id = product.family.id if product.family else None
                    subfamily_id = product.subfamily.id if product.subfamily else None
                    catalog_id = product.catalog.id if product.catalog else None
                    if not (catalog_id and family_id and subfamily_id):
                        continue
                    await cur.execute(
                        GET_PRODUCTS_SUMMARY_BY_RANGE(schema),
                        (catalog_id, family_id, subfamily_id, date_start_val, date_end_val)
                    )
                    row = await cur.fetchone()
                    if row:
                        results[abc.id] = {
                            "total_amount": float(row[0]) if row[0] is not None else 0,
                            "profit": float(row[1]) if row[1] is not None else 0,
                            "units_sold": float(row[2]) if row[2] is not None else 0,
                        }
        finally:
            conn.close()
        return results

    # Recopilar datos ABC por año para stats
    def get_abc_stats_by_year(product_abc, years):
        stats = {}
        for y in years:
            try:
                abc = ProductABC.objects.filter(
                    product=product_abc.product,
                    enterprise=product_abc.enterprise,
                    year=int(y)
                ).first()
                if abc:
                    stats[str(y)] = {
                        "sales_percentage": float(abc.sales_percentage) if abc.sales_percentage is not None else None,
                        "acc_sales_percentage": float(abc.acc_sales_percentage) if abc.acc_sales_percentage is not None else None,
                        "sold_abc": abc.sold_abc,
                        "profit_percentage": float(abc.profit_percentage) if abc.profit_percentage is not None else None,
                        "acc_profit_percentage": float(abc.acc_profit_percentage) if abc.acc_profit_percentage is not None else None,
                        "profit_abc": abc.profit_abc,
                        "top_products": abc.top_products,
                    }
            except Exception as e:
                continue
        return stats

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    totals_map = loop.run_until_complete(fetch_totals())
    loop.close()

    data = []
    for abc in all_products:
        product = abc.product
        totals = totals_map.get(abc.id, {"total_amount": 0, "profit": 0, "units_sold": 0})
        base = {
            "id": abc.id,
            "enterprise": abc.enterprise,
            "product_id": product.id,
            "product_code": product.code,
            "product_description": product.description,
            "family_name": product.family.name if product.family else None,
            "subfamily_name": product.subfamily.name if product.subfamily else None,
            "brand_name": product.brand.name if product.brand else None,
            "catalog_name": product.catalog.name if product.catalog else None,
            "total_amount": totals["total_amount"],
            "profit": totals["profit"],
            "units_sold": totals["units_sold"],
            "year": abc.year,
            "last_update": abc.last_update.isoformat() if abc.last_update else None,
        }

            # stats por año
        base["stats"] = get_abc_stats_by_year(abc, years)

        data.append(base)

    # Ordenar primero por total_amount descendente, luego por product_description ascendente
    data.sort(key=lambda x: (-x.get("total_amount", 0), (x.get("product_description") or "").lower()))

    page = int(request.GET.get('page', 1))
    per_page = int(request.GET.get('per_page', 10))
    total_items = len(data)
    num_pages = (total_items + per_page - 1) // per_page
    start = (page - 1) * per_page
    end = start + per_page
    page_data = data[start:end]

    pagination_data = {
        "num_pages": num_pages,
        "page": page,
    }

    return JsonResponse({
        "data": page_data,
        "pagination": pagination_data
    })

@csrf_exempt
def get_product_catalog(request):
    if request.method != 'GET':
        return HttpResponseNotAllowed(['GET'])

    enterprise = request.GET.get('enterprise', '').strip()
    family = request.GET.get('family', '').strip()
    subfamily = request.GET.get('subfamily', '').strip()
    brand = request.GET.get('brand', '').strip()
    catalog = request.GET.get('catalog', '').strip()
    description = request.GET.get('description', '').strip() 
    page = int(request.GET.get('page', 1))
    per_page = int(request.GET.get('per_page', 10))


    qs = Product.objects.all()
    if enterprise:
        qs = qs.filter(enterprise=enterprise)

    if family:
        qs = qs.filter(family__name__icontains=family)
    
    if subfamily:
        qs = qs.filter(subfamily__name__icontains=subfamily)
    
    if brand:
        qs = qs.filter(brand__name__icontains=brand)
    
    if catalog:
        qs = qs.filter(catalog__name__icontains=catalog)
    
    if description:
        qs = qs.filter(description__icontains=description)

    qs = qs.order_by('code')
    qs = qs.select_related('family', 'subfamily', 'brand', 'catalog')
        
    paginator = Paginator(qs.values('code', 'description', 'family__name', 'subfamily__name', 'brand__name', 'catalog__name'), per_page)
    page_obj = paginator.get_page(page)

    pagination = {
        "num_pages": paginator.num_pages,
        "page": page_obj.number,
    }

    return JsonResponse({
        "data": list(page_obj),
        "pagination": pagination
    })



#Una vista que acepte a un archivo excel y verifique que tiene una columna que se llame 
# Catalogo o Catálogo y otra que se llame Código o Codigo

@csrf_exempt
def upload_catalog_file(request):
    if request.method != 'POST':
        return HttpResponseNotAllowed(['POST'])

    file = request.FILES.get('file')
    enterprise = request.POST.get('enterprise')

    if not file:
        return JsonResponse({"success": False, "error": "No se recibió ningún archivo."}, status=400)

    # Validar extensión
    filename = file.name
    if not (filename.endswith('.xlsx') or filename.endswith('.xls')):
        return JsonResponse({"success": False, "error": "El archivo debe ser un Excel (.xlsx o .xls)."}, status=400)

    # Validar la empresa
    if not enterprise:
        return JsonResponse({"success": False, "error": "No se proporcionó una empresa."}, status=400)
    
    enterprise_data = enterprises.get(enterprise)
    if not enterprise_data:
        return JsonResponse({"success": False, "error": "Empresa no válida."}, status=400)
    
    enterprise = enterprise_data.schema
    
    # Leer archivo excel
    try:
        df = pd.read_excel(file)
    except Exception:
        return JsonResponse({"success": False, "error": "No se pudo leer el archivo Excel. Asegúrate de que el archivo no esté dañado."}, status=400)

    # Normalizar nombres de columnas
    columns = [str(col).strip() for col in df.columns]
    col_catalogo = None
    col_codigo = None
    for col in columns:
        if col in ['catalogo', 'catálogo', 'Catalogo', 'Catálogo']:
            col_catalogo = col
        if col in ['codigo', 'código', 'Codigo', 'Código']:
            col_codigo = col

    if not col_catalogo or not col_codigo:
        return JsonResponse({
            "success": False,
            "error": "El archivo debe contener las columnas 'Catalogo' o 'Catálogo' y 'Codigo' o 'Código'."
        }, status=400)
    
    # Solo dejar las columnas de catalogo y codigo
    df = df[[col_catalogo, col_codigo]]
    df = df.drop_duplicates()
    df = df.dropna()
    df = df.reset_index(drop=True)

    #renombrar columnas a code y catalog
    df = df.rename(columns={col_catalogo: 'catalog', col_codigo: 'code'})

    

    # Guardar archivo en apps/static/data/file.xlsx
    static_data_dir = os.path.join('.', 'apps', 'static', 'data')
    os.makedirs(static_data_dir, exist_ok=True)
    save_path = os.path.join(static_data_dir, f'{enterprise}_catalog.xlsx')
    try:
        df.to_excel(save_path, index=False)
    except Exception:
        return JsonResponse({"success": False, "error": "No se pudo guardar el archivo en el servidor."}, status=500)

    return JsonResponse({"success": True, "message": "Archivo cargado y validado correctamente. Los cambios serán visibles a partir de mañana."})