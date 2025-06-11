def SELECT_FAMILIES(schema: str):
    return f"""
SELECT nombre, id FROM {schema}.admintotal_linea
"""

def SELECT_SUBFAMILIES(schema: str):
    return f"""
SELECT nombre, id FROM {schema}.admintotal_sublinea
"""

def SELECT_BRANDS(schema: str):
    return f"""
SELECT nombre, id FROM {schema}.admintotal_productomarca
"""

def SELECT_PRODUCTS(schema: str, limit: int, offset: int, with_date: bool):

    date_clause = "AND (p.creado AT TIME ZONE 'UTC' > $1 OR p.modificado AT TIME ZONE 'UTC' > $1)" if with_date else ""

    return f"""
SELECT p.id, p.codigo, p.descripcion, p.creado, p.marca_id, p.linea_id, p.sublinea_id
FROM {schema}.admintotal_producto as p
WHERE 
    p.creado IS NOT NULL
    {date_clause} 
ORDER BY p.id
LIMIT {limit} OFFSET {offset};
"""

def SELECT_MOVEMENTS(schema: str, limit: int, offset: int, with_date: bool):
    
    date_clause = "WHERE (p.fecha AT TIME ZONE 'UTC') > $1" if with_date else ""

    return f"""
 SELECT m.poliza_ptr_id, p.fecha,
 	   m.es_orden, m.es_entrada, m.es_salida, m.pendiente, 
      m.folio, m.serie,
 	   m.folio_adicional, m.nota_pagada,   
 	   CASE
        		WHEN m.metodo_pago IS NULL OR m.metodo_pago = '' OR m.metodo_pago = '99' THEN '99 Por definir'
             WHEN m.metodo_pago = '01' THEN '01 Efectivo'
             WHEN m.metodo_pago = '02' THEN '02 Cheque nominativo'
             WHEN m.metodo_pago = '03' THEN '03 Transferencia electrónica de fondos'
             WHEN m.metodo_pago = '04' THEN '04 Tarjeta de crédito'
             WHEN m.metodo_pago = '05' THEN '05 Monedero electrónico'
             WHEN m.metodo_pago = '06' THEN '06 Dinero electrónico'
             WHEN m.metodo_pago = '08' THEN '08 Vales de despensa'
             WHEN m.metodo_pago = '12' THEN '12 Dación en pago'
             WHEN m.metodo_pago = '13' THEN '13 Pago por subrogación'
             WHEN m.metodo_pago = '14' THEN '14 Pago por consignación'
             WHEN m.metodo_pago = '15' THEN '15 Condonación'
             WHEN m.metodo_pago = '17' THEN '17 Compensación'
             WHEN m.metodo_pago = '23' THEN '23 Novación'
             WHEN m.metodo_pago = '24' THEN '24 Confusión'
             WHEN m.metodo_pago = '25' THEN '25 Remisión de deuda'
             WHEN m.metodo_pago = '26' THEN '26 Prescripción o caducidad'
             WHEN m.metodo_pago = '27' THEN '27 A satisfacción del acreedor'
             WHEN m.metodo_pago = '28' THEN '28 Tarjeta de débito'
             WHEN m.metodo_pago = '29' THEN '29 Tarjeta de servicio'
             WHEN m.metodo_pago = '30' THEN '30 Aplicación de anticipos'
             WHEN m.metodo_pago = '31' THEN '31 Intermediario pagos'
           END AS metodo_pago,
       ROUND(SUM(md.cantidad), 5) AS cantidad,
       CASE
           WHEN ROUND(SUM(md.cantidad*md.factor_um), 5) IS NULL THEN 0 
           ELSE ROUND(SUM(md.cantidad*md.factor_um), 5)
       END AS cantidad_total,
 	   ROUND(m.importe, 5) AS importe, 
        ROUND(m.iva, 5) AS iva, 
        ROUND(m.descuento, 5) AS descuento,
 	   ROUND(m.importe_descuento, 5) AS importe_descuento,
        ROUND(m.total, 5) AS total,
        ROUND(m.costo_venta, 5) AS costo_venta, 
        ROUND((m.importe - m.costo_venta), 5) AS utilidad,
        mx.nombre AS moneda,
        m.orden_id as orden_id,
 	   m.tipo_movimiento,
 	   m.cancelado
 	   FROM
        {schema}.admintotal_movimiento m
 	   INNER JOIN
        {schema}.admintotal_poliza p ON p.id = m.poliza_ptr_id
 	   LEFT JOIN
        {schema}.admintotal_movimientodetalle md ON md.movimiento_id = m.poliza_ptr_id
 	   LEFT JOIN
        {schema}.admintotal_moneda mx ON mx.id = m.moneda_id
        {date_clause}
 	   GROUP BY
 	   		m.poliza_ptr_id,
 			p.fecha,
 			m.folio,
 			m.nota_pagada,
 			m.importe,
 			m.iva,
 			m.total,
 			m.costo_venta,
 			m.cancelado,
 			mx.nombre
        ORDER BY p.fecha
        LIMIT {limit}
        OFFSET {offset};
"""

def SELECT_MOVEMENT_DETAILS(schema: str, limit: int, offset: int, with_date: bool):

    date_clause = "WHERE (md.fecha AT TIME ZONE 'UTC') > $1" if with_date else ""

    return f"""
SELECT md.id, md.movimiento_id, md.producto_id, md.fecha, um.nombre, 
 	  md.cantidad, md.factor_um, md.precio_unitario,
 	  CASE
          WHEN ROUND(SUM(md.cantidad*md.factor_um), 5) IS NULL THEN 0 
          ELSE ROUND(SUM(md.cantidad*md.factor_um), 5)
       END AS cantidad_total,
 	  md.importe, md.iva, md.descuento, md.existencia, md.costo_venta, md.cancelado
FROM 
    {schema}.admintotal_movimientodetalle md
 	 LEFT JOIN
    {schema}.admintotal_um um 
	  	ON um.id = md.um_id
{date_clause}
GROUP BY
 	md.id,
 	um.nombre
ORDER BY md.id
LIMIT {limit}
OFFSET {offset};
"""
