def UPSERT_FAMILIES(enterprise: str):
    return (
f"""
INSERT INTO family (name, id_admin, enterprise) VALUES (%s, %s, '{enterprise}')
ON DUPLICATE KEY
UPDATE name=VALUES(name);
"""
    )

def UPSERT_SUBFAMILIES(enterprise: str):
    return (
f"""
INSERT INTO subfamily (name, id_admin, enterprise) VALUES (%s, %s, '{enterprise}')
ON DUPLICATE KEY
UPDATE name=VALUES(name);
"""
    )

def UPSERT_BRANDS(enterprise: str): 
    return (
f"""
INSERT INTO brand (name, id_admin, enterprise) VALUES (%s, %s, '{enterprise}')
ON DUPLICATE KEY
UPDATE name=VALUES(name);
"""
    )

def UPSERT_CATALOGS():
    return(
"""
INSERT IGNORE INTO catalog (name) VALUES(%s);
"""
    )

def UPSERT_PRODUCTS(enterprise: str):
    return (
f"""
INSERT INTO product (id_admin, code, description, create_date, brand_id, family_id, subfamily_id, enterprise)
VALUES (%s, %s, %s, %s,
    (SELECT id FROM brand WHERE id_admin = %s AND enterprise = '{enterprise}'),
    (SELECT id FROM family WHERE id_admin = %s AND enterprise = '{enterprise}'),
    (SELECT id FROM subfamily WHERE id_admin = %s AND enterprise = '{enterprise}'),
    %s
)
ON DUPLICATE KEY UPDATE 
    description=VALUES(description), code=VALUES(code),
    brand_id=VALUES(brand_id), family_id=VALUES(family_id),
    subfamily_id=VALUES(subfamily_id);
"""
    )
