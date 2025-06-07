def UPSERT_FAMILIES():
    return (
"""
INSERT INTO family (name, id_admin) VALUES (%s, %s)
ON DUPLICATE KEY
UPDATE name=VALUES(name);
"""
    )

def UPSERT_SUBFAMILIES():
    return (
"""
INSERT INTO subfamily (name, id_admin) VALUES (%s, %s)
ON DUPLICATE KEY
UPDATE name=VALUES(name);
"""
    )

def UPSERT_BRANDS(): 
    return (
"""
INSERT INTO brand (name, id_admin) VALUES (%s, %s)
ON DUPLICATE KEY
UPDATE name=VALUES(name);
"""
    )
