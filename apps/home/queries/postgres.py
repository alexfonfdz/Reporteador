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

