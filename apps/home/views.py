# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from django import template
from django.contrib.auth.decorators import login_required
from django.http import HttpResponse, HttpResponseRedirect
from django.template import loader
from django.test.testcases import asyncio
from django.urls import reverse
from django.http import JsonResponse
from django.core.paginator import Paginator
from datetime import datetime, date
from django.views.decorators.csrf import csrf_exempt
from pandas.core.accessor import delegate_names
from core.settings import ENV_PSQL_NAME, ENV_PSQL_USER, ENV_PSQL_PASSWORD, ENV_PSQL_HOST, ENV_PSQL_PORT, ENV_PSQL_DB_SCHEMA, ENV_MYSQL_NAME, ENV_MYSQL_USER, ENV_MYSQL_PASSWORD, ENV_MYSQL_HOST, ENV_MYSQL_PORT
from django.conf import settings
import os
from .product_abc_logic import upsert_families, upsert_subfamilies, upsert_products, upsert_catalogs, upsert_product_catalogs, upsert_all
import mysql.connector as m
import json
import asyncio
import psycopg2 as p
from os import path

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
        catalogo_key = "Catalogo"

        # Leer el archivo Excel
        df = pd.read_excel(file_path)

        df = df.rename(columns={
            "Descripción 2": "Catalogo", 
            "Línea": "Familia",
            "Sublínea": "Subfamilia",
            "Código": "Codigo"
        })

        # Validar que las columnas necesarias existan
        if catalogo_key not in df.columns:
            raise ValueError(f"La columna {catalogo_key} no existe en el archivo Excel.")

        # Reemplazar valores NaN con "0"        
        df[catalogo_key] = df[catalogo_key].fillna('0')

        # Convertir valores a cadenas y eliminar espacios
        df[catalogo_key] = df[catalogo_key].astype(str).str.strip()

        # Filtrar filas donde "Catalogo" no sea válido
        df = df[~df[catalogo_key].isin(["0", ""])]

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
        catalogo_key = "Catalogo"
        subfamilia_key = "Subfamilia"

        # Leer el archivo Excel
        df = pd.read_excel(file_path)

        # Validar que las columnas necesarias existan
        if subfamilia_key not in df.columns or catalogo_key not in df.columns:
            raise ValueError(f"Las columnas {subfamilia_key} y {catalogo_key} deben existir en el archivo Excel.")


        # Agrupar por "Catalogo" y contar las subfamilias únicas
        category_group = df.groupby(catalogo_key)[subfamilia_key].nunique()

        # Aplicar la lógica para actualizar las categorías
        def update_category(row):
            if category_group[row[catalogo_key]] > 1:
                return f"{row[catalogo_key]} {row[subfamilia_key]}"
            return row[catalogo_key]

        df[catalogo_key] = df.apply(update_category, axis=1)

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
        # Use absolute paths with Django's STATICFILES_DIRS setting
        static_data_dir = os.path.join('.', 'apps', 'static', 'data')
        
        # Check if the static data directory exists
        if not os.path.exists(static_data_dir):
            os.makedirs(static_data_dir, exist_ok=True)
           
        file_path = os.path.join(static_data_dir, 'Catalogo.para.agrupaciones.MARW.xlsx')
        output_filtered_file = os.path.join(static_data_dir, 'Catalogo.para.agrupaciones.MARW-Filtrado.xlsx')
        output_file = os.path.join(static_data_dir, 'Catalogo.para.agrupaciones.MARW-Archivo_limpio.xlsx')
        
        if not os.path.exists(file_path):
            # Try alternate path approaches
            alt_path = os.path.abspath('./apps/static/data/Catalogo.para.agrupaciones.MARW.xlsx')
            print(f"Trying alternate path: {alt_path}")
            if os.path.exists(alt_path):
                print(f"File exists at alternate path")
                file_path = alt_path
            else:
                raise FileNotFoundError(f"The file {file_path} does not exist. Also tried {alt_path}")


        # Leer y filtrar el archivo Excel
        try:
            read_and_filter_excel(file_path, output_filtered_file)
        except Exception as excel_read_error:
            # If pandas error contains specific details, print them
            if hasattr(excel_read_error, 'args') and len(excel_read_error.args) > 0:
                print(f"Pandas error details: {excel_read_error.args[0]}")
            raise


        # Agrupar los catálogos y guardar en un archivo Excel

        try:

            process_and_update_categories(output_filtered_file, output_file)

        except Exception as processingException:
            print(f"Exception while processing the dataframe: {processingException}")


        # Leer el archivo resultante y extraer los catálogos
        df_updated = pd.read_excel(output_file)

        # Validar que las columnas necesarias existan
        print("Columnas disponibles en el archivo Excel:", df_updated.columns)

        if "Catalogo" not in df_updated.columns:
            raise ValueError("La columna 'Catalogo' no existe en el archivo Excel actualizado.")
        if "Familia" not in df_updated.columns:
            raise ValueError("La columna 'Familia' no existe en el archivo Excel actualizado.")
        if "Subfamilia" not in df_updated.columns:
            raise ValueError("La columna 'Subfamilia' no existe en el archivo Excel actualizado.")

        # Filtrar las columnas necesarias
        df_updated = df_updated[["Catalogo", "Familia", "Subfamilia"]]

        # Obtener los catálogos únicos
        catalogos = df_updated.drop_duplicates().values.tolist()

        async def delegate_upsertion():
            chunk_size = 500
            complete_chunks_amount = len(catalogos) // chunk_size
            remainder = len(catalogos) % chunk_size

            async with asyncio.TaskGroup() as tg:
                for i in range(complete_chunks_amount):
                    tg.create_task(
                        upsert_catalogs(catalogos[i*chunk_size:(i+1)*chunk_size])
                    )

                if remainder:
                    tg.create_task(
                        upsert_catalogs(catalogos[complete_chunks_amount * chunk_size:])
                    )


        # Insertar los catálogos en la base de datos uno por uno

        asyncio.run(delegate_upsertion())

        return JsonResponse({'msg': "Se han guardado los archivos filtrados, agrupados e insertado los catálogos satisfactoriamente"}, safe=False)
    except ValueError as ve:
        return JsonResponse({'error': str(ve)}, status=400)
    except FileNotFoundError as fnf:
        return JsonResponse({
            'error': str(fnf),
            'details': 'File not found error - check if the file exists at the specified path.',
        }, status=404)
    except RuntimeError as re:
        return JsonResponse({'error': str(re)}, status=500)
    except Exception as e:
        import traceback
        stack_trace = traceback.format_exc()
        return JsonResponse({
            'error': f"Error inesperado: {e}",
            'stack_trace': stack_trace,
            'error_type': str(type(e))
        }, status=500)
    
@csrf_exempt
def insert_product_catalog(request):
    """
    Endpoint para insertar o actualizar la información de la tabla product_catalog.
    """
    try:
        # Obtener el año del request
        data = json.loads(request.body)
        year = data.get('year')

        # Validar que el año esté presente
        if not year:
            return JsonResponse({'error': 'El parámetro "year" es obligatorio.'}, status=400)

        # Leer el archivo Excel
        static_data_dir = os.path.join('.', 'apps', 'static', 'data')
        file_path = os.path.join(static_data_dir, 'Catalogo.para.agrupaciones.MARW-Archivo_limpio.xlsx')
        
        # Check if the file exists
        if not os.path.exists(file_path):
            print("The file doesnt exist")
            return JsonResponse({'error': f'The file {file_path} does not exist.'}, status=404)
        df = pd.read_excel(file_path)

        # Imprimir las columnas para depuración
        print("Columnas encontradas en el archivo Excel:", df.columns)

        # Validar que las columnas necesarias existan
        if "Catalogo" not in df.columns or "Codigo" not in df.columns:
            return JsonResponse({'error': f'El archivo Excel debe contener las columnas "Catalogo" y "Codigo". Columnas encontradas: {list(df.columns)}'}, status=400)
        print("El excel satisface las columnas")

        product_catalog = df.to_dict(orient='records')

        asyncio.run(upsert_product_catalogs(product_catalog, year))   

        


        return JsonResponse({'msg': 'Los registros se han procesado correctamente en product_catalog.'}, status=200)

    except json.JSONDecodeError:
        return JsonResponse({'error': 'El cuerpo de la solicitud debe ser un JSON válido.'}, status=400)
    except Exception as e:
        return JsonResponse({'error': f'Error inesperado: {e}'}, status=500)
    
@csrf_exempt
def insert_data_to_product_abc(request):
    try:
        data = json.loads(request.body)
        year = data.get('year')
        enterprise = data.get('enterprise')        
        
        asyncio.run(upsert_all(year, enterprise))
        return JsonResponse({'msg': 'Los registros se han procesado correctamente en product_abc.'}, status=200)
    except Exception as e:
        return JsonResponse({'error': f'Error al insertar la información en product_abc: {e}'}, status=500)


