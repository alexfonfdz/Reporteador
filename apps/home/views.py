# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from django import template
from django.contrib.auth.decorators import login_required
from django.http import HttpResponse, HttpResponseRedirect
from django.template import loader
from django.urls import reverse
from calendar import month_name
from datetime import datetime
from django.http import JsonResponse
from django.core.paginator import Paginator
from calendar import month_name
from datetime import datetime
from django.views.decorators.csrf import csrf_exempt
from core.settings import ENV_PSQL_NAME, ENV_PSQL_USER, ENV_PSQL_PASSWORD, ENV_PSQL_HOST, ENV_PSQL_PORT, ENV_PSQL_DB_SCHEMA
from .product_abc_logic import upsert_families, upsert_subfamilies, upsert_products, upsert_catalogs
import json
import asyncio
import psycopg2 as p
import pandas as pd


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


# ==================================================================================================== # 
@csrf_exempt
def get_families_from_admintotal(request):
    try:
        asyncio.run(upsert_families())        
        asyncio.run(upsert_subfamilies())

        return JsonResponse({'msg': "Se han insertado las familias satisfactoriamente"}, safe=False)
    except Exception as e:
        return JsonResponse({'Hubo un error al insertar las familias': str(e)}, status=500)
    
@csrf_exempt
def get_products_from_admintotal(request):
    try:
        asyncio.run(upsert_products())

        return JsonResponse({'msg': "Se han insertado los productos satisfactoriamente"}, safe=False)
    except Exception as e:
        return JsonResponse({'Hubo un error al insertar los productos': str(e)}, status=500)
    

def read_and_filter_excel(file_path, output_filtered_file):
    """
    Lee y filtra un archivo Excel eliminando filas donde "Catalogo" es 0 o vacío.
    Guarda el resultado filtrado en un archivo Excel.

    Args:
        file_path (str): Ruta del archivo Excel de entrada.
        sheet_name (str): Nombre de la hoja a leer.
        output_filtered_file (str): Ruta del archivo Excel filtrado de salida.

    Returns:
        pd.DataFrame: DataFrame filtrado.
    """
    try:
        # Leer el archivo Excel
        df = pd.read_excel(file_path)

        # Validar que las columnas necesarias existan
        if "Catalogo" not in df.columns:
            raise ValueError("La columna 'Catalogo' no existe en el archivo Excel.")

        # Reemplazar valores NaN con "0"
        df['Catalogo'].fillna('0', inplace=True)

        # Convertir valores a cadenas y eliminar espacios
        df["Catalogo"] = df["Catalogo"].astype(str).str.strip()

        # Filtrar filas donde "Catalogo" no sea válido
        df = df[~df["Catalogo"].isin(["0", ""])]

        # Guardar el DataFrame filtrado en un archivo Excel
        df.to_excel(output_filtered_file, index=False)

        return df
    except Exception as e:
        raise RuntimeError(f"Error al leer y filtrar el archivo Excel: {e}")


def process_and_update_categories(file_path, output_file):
    """
    Procesa un archivo Excel y actualiza los valores de la columna "Catalogo" 
    según la lógica proporcionada.

    Args:
        file_path (str): Ruta del archivo Excel de entrada.
        output_file (str): Ruta del archivo Excel de salida con las categorías actualizadas.

    Returns:
        pd.DataFrame: DataFrame con las categorías actualizadas.
    """
    try:
        # Leer el archivo Excel
        df = pd.read_excel(file_path)

        # Validar que las columnas necesarias existan
        if "Subfamilia" not in df.columns or "Catalogo" not in df.columns:
            raise ValueError("Las columnas 'Subfamilia' y 'Catalogo' deben existir en el archivo Excel.")

        # Agrupar por "Catalogo" y contar las subfamilias únicas
        category_group = df.groupby("Catalogo")["Subfamilia"].nunique()

        # Aplicar la lógica para actualizar las categorías
        def update_category(row):
            if category_group[row["Catalogo"]] > 1:
                return f"{row['Catalogo']} {row['Subfamilia']}"
            return row["Catalogo"]

        df["Catalogo"] = df.apply(update_category, axis=1)

        # Guardar el DataFrame actualizado en un nuevo archivo Excel
        df.to_excel(output_file, index=False)

        return df
    except Exception as e:
        raise RuntimeError(f"Error al procesar y actualizar las categorías: {e}")


@csrf_exempt
def get_catalogs_from_admintotal(request):
    """
    Endpoint para procesar un archivo Excel, filtrar los catálogos y agruparlos.
    Guarda los resultados en dos archivos Excel y los inserta en la base de datos.

    Args:
        request (HttpRequest): Solicitud HTTP.

    Returns:
        JsonResponse: Respuesta JSON con el resultado de la operación.
    """
    try:
        # Rutas de los archivos
        file_path = 'apps/static/data/Catalogo.para.agrupaciones.MARW.xlsx'
        output_file = 'apps/static/data/Catalogo_Catalogos_Actualizadas.xlsx'
        output_filtered_file = 'apps/static/data/Catalogo_Filtrado.xlsx'
        output_grouped_file = 'apps/static/data/Catalogos_Agrupados.xlsx'
        

        # Leer y filtrar el archivo Excel
        df = read_and_filter_excel(file_path, output_filtered_file)

        # Agrupar los catálogos y guardar en un archivo Excel
        updated_df = process_and_update_categories(file_path, output_file)

        return JsonResponse({'msg': "Se han guardado los archivos filtrados y agrupados satisfactoriamente"}, safe=False)
    except ValueError as ve:
        return JsonResponse({'error': str(ve)}, status=400)
    except RuntimeError as re:
        return JsonResponse({'error': str(re)}, status=500)
    except Exception as e:
        return JsonResponse({'error': f"Error inesperado: {e}"}, status=500)