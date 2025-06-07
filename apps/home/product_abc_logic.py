from core.settings import ENV_PSQL_NAME, ENV_PSQL_USER, ENV_PSQL_PASSWORD, ENV_PSQL_HOST, ENV_PSQL_PORT, ENV_PSQL_DB_SCHEMA, ENV_MYSQL_HOST, ENV_MYSQL_PORT, ENV_MYSQL_NAME, ENV_MYSQL_USER, ENV_MYSQL_PASSWORD, ENV_UPDATE_ALL_DATES
import psycopg2 as p
import mysql.connector as m
import datetime
import pandas as pd

enterprises = {
    "MR DIESEL" : {
        "schema": ENV_PSQL_DB_SCHEMA,
        "name": ENV_PSQL_NAME,
        "user": ENV_PSQL_USER,
        "password": ENV_PSQL_PASSWORD,
        "host": ENV_PSQL_HOST,
        "port": ENV_PSQL_PORT
    }
}

### Enterprises funciona para ir agregando mas empresas y sus respectivos datos de conexion a la base de datos, tomarlo en cuenta al traer los datos
### Existe la variable ENV_UPDATE_ALL_DATES que si es True no se toma en cuenta el ultimo año o mes o fecha de actualización y se actualizan todos los registros

###### La forma de trabajo y el orden de inserción principal es el siguiente: 
# Traer familias:
### SELECT nombre, id FROM {schema de la empresa}.admintotal_linea

# Traer subfamilias:
### SELECT nombre, id FROM {schema de la empresa}.admintotal_sublinea

# Traer marcas (pueden ser null porque no todos los productos tienen marca):
### SELECT nombre, id FROM {schema de la empresa}.admintotal_productomarca

# Traer productos:
### SELECT p.sublinea_id, p.linea_id, p.marca_id, p.id, p.codigo, p.descripcion, p.creado FROM {schema de la empresa}.admintotal_producto as p

# Insertar catalogos (pueden ser null porque se insertan del excel):
### INSERT INTO catalog (name) VALUES (%s) 

# Insertar movimientos (los puse en orden según yo):
# SELECT m.poliza_ptr_id, p.fecha,
# 	   m.es_orden, m.es_entrada, m.es_salida, m.pendiente, 
#      m.folio, m.serie,
# 	   m.folio_adicional, m.nota_pagada,   
# 	   CASE
#        		WHEN m.metodo_pago IS NULL OR m.metodo_pago = '' OR m.metodo_pago = '99' THEN '99 Por definir'
#             WHEN m.metodo_pago = '01' THEN '01 Efectivo'
#             WHEN m.metodo_pago = '02' THEN '02 Cheque nominativo'
#             WHEN m.metodo_pago = '03' THEN '03 Transferencia electrónica de fondos'
#             WHEN m.metodo_pago = '04' THEN '04 Tarjeta de crédito'
#             WHEN m.metodo_pago = '05' THEN '05 Monedero electrónico'
#             WHEN m.metodo_pago = '06' THEN '06 Dinero electrónico'
#             WHEN m.metodo_pago = '08' THEN '08 Vales de despensa'
#             WHEN m.metodo_pago = '12' THEN '12 Dación en pago'
#             WHEN m.metodo_pago = '13' THEN '13 Pago por subrogación'
#             WHEN m.metodo_pago = '14' THEN '14 Pago por consignación'
#             WHEN m.metodo_pago = '15' THEN '15 Condonación'
#             WHEN m.metodo_pago = '17' THEN '17 Compensación'
#             WHEN m.metodo_pago = '23' THEN '23 Novación'
#             WHEN m.metodo_pago = '24' THEN '24 Confusión'
#             WHEN m.metodo_pago = '25' THEN '25 Remisión de deuda'
#             WHEN m.metodo_pago = '26' THEN '26 Prescripción o caducidad'
#             WHEN m.metodo_pago = '27' THEN '27 A satisfacción del acreedor'
#             WHEN m.metodo_pago = '28' THEN '28 Tarjeta de débito'
#             WHEN m.metodo_pago = '29' THEN '29 Tarjeta de servicio'
#             WHEN m.metodo_pago = '30' THEN '30 Aplicación de anticipos'
#             WHEN m.metodo_pago = '31' THEN '31 Intermediario pagos'
#           END AS metodo_pago,
#       ROUND(SUM(md.cantidad), 5) AS cantidad,
#       CASE
#           WHEN ROUND(SUM(md.cantidad*md.factor_um), 5) IS NULL THEN 0 
#           ELSE ROUND(SUM(md.cantidad*md.factor_um), 5)
#       END AS cantidad_total,
# 	   ROUND(m.importe, 5) AS importe, 
#        ROUND(m.iva, 5) AS iva, 
#        ROUND(m.descuento, 5) AS descuento,
# 	   ROUND(m.importe_descuento, 5) AS importe_descuento,
#        ROUND(m.total, 5) AS total,
#        ROUND(m.costo_venta, 5) AS costo_venta, 
#        ROUND((m.importe - m.costo_venta), 5) AS utilidad,
#        mx.nombre AS moneda,
#        m.orden_id as orden_id,
# 	   m.tipo_movimiento,
# 	   m.cancelado
# 	   FROM
# 	   		{schema de la empresa}.admintotal_movimiento m
# 	   INNER JOIN
# 	   		{schema de la empresa}.admintotal_poliza p ON p.id = m.poliza_ptr_id
# 	   LEFT JOIN
# 	   		{schema de la empresa}.admintotal_movimientodetalle md ON md.movimiento_id = m.poliza_ptr_id
# 	   LEFT JOIN
# 	   		{schema de la empresa}.admintotal_moneda mx ON mx.id = m.moneda_id
# 	   GROUP BY
# 	   		m.poliza_ptr_id,
# 			p.fecha,
# 			m.folio,
# 			m.nota_pagada,
# 			m.importe,
# 			m.iva,
# 			m.total,
# 			m.costo_venta,
# 			m.cancelado,
# 			mx.nombre

# Insertar movimientos detalle (insertar todo aunque producto_id sea null):
# SELECT md.id, md.movimiento_id, md.producto_id, md.fecha, um.nombre, 
# 	  md.cantidad, md.factor_um, md.precio_unitario,
# 	  CASE
#          WHEN ROUND(SUM(md.cantidad*md.factor_um), 5) IS NULL THEN 0 
#          ELSE ROUND(SUM(md.cantidad*md.factor_um), 5)
#       END AS cantidad_total,
# 	  md.importe, md.iva, md.descuento, md.existencia, md.costo_venta, md.cancelado
# 	  FROM 
# 	  		prueba9.admintotal_movimientodetalle md
# 	  LEFT JOIN
# 	  		prueba9.admintotal_um um ON um.id = md.um_id
# 	  GROUP BY
# 	  		md.id,
# 			um.nombre


### Avisarme para iniciar lo del análisis ABC y producto ABC si se necesita ayuda
# Insertar en el año ej.2022, 2023, 2024 del producto abc todos los productos que se hayan insertado en ese año para abajo solamente
# El timestap del last_update se actualiza al terminar de insertar todos los datos disponibles en el año de x producto

# Insertar en el año ej.2022, 2023, 2024 del análisis abc todos los catalogos que tengan al menos un producto en ese año para abajo solamente (si un catalogo tiene varias familias y varias subfamilias, se inserta uno diferenta para cada agrupacion de estas)
# El timestap del last_update se actualiza al terminar de insertar todos los datos disponibles en el año de x catalogo
