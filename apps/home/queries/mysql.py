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
