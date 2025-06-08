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

def SELECT_PRODUCTS(schema: str, limit: int, offset: int):
    return f"""
SELECT p.id, p.codigo, p.descripcion, p.creado, p.marca_id, p.linea_id, p.sublinea_id
FROM {schema}.admintotal_producto as p
WHERE p.creado IS NOT NULL
ORDER BY p.id
LIMIT {limit} OFFSET {offset};
"""


