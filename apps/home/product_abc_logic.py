from core.settings import ENV_PSQL_NAME, ENV_PSQL_USER, ENV_PSQL_PASSWORD, ENV_PSQL_HOST, ENV_PSQL_PORT, ENV_PSQL_DB_SCHEMA, ENV_MYSQL_HOST, ENV_MYSQL_PORT, ENV_MYSQL_NAME, ENV_MYSQL_USER, ENV_MYSQL_PASSWORD
import psycopg2 as p
# import mysql.connector as m
import mysql.connector as m

def get_families():
    try:
        conn = p.connect(dbname= ENV_PSQL_NAME, user=ENV_PSQL_USER, host=ENV_PSQL_HOST, password=ENV_PSQL_PASSWORD, port=ENV_PSQL_PORT)
        cursor = conn.cursor()
        cursor.execute(f"SELECT nombre FROM {ENV_PSQL_DB_SCHEMA}.admintotal_linea")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD, database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)
        cursor = conn.cursor()
        cursor.execute("SELECT id, name FROM family")
        rows2 = cursor.fetchall()
        for row in rows:
            if row[0] not in [r[1] for r in rows2]:
                cursor.execute(f"INSERT INTO family (name) VALUES ('{row[0]}')")
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        if conn:
            conn.close()
        if cursor:
            cursor.close()

def get_subfamilies():
    try:
        conn = p.connect(dbname= ENV_PSQL_NAME, user=ENV_PSQL_USER, host=ENV_PSQL_HOST, password=ENV_PSQL_PASSWORD, port=ENV_PSQL_PORT)
        cursor = conn.cursor()
        cursor.execute(f"SELECT nombre FROM {ENV_PSQL_DB_SCHEMA}.admintotal_sublinea")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD, database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)
        cursor = conn.cursor()
        cursor.execute("SELECT id, name FROM subfamily")
        rows2 = cursor.fetchall()
        for row in rows:
            if row[0] not in [r[1] for r in rows2]:
                cursor.execute(f"INSERT INTO subfamily (name) VALUES ('{row[0]}')")
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        if conn:
            conn.close()
        if cursor:
            cursor.close()

def get_products():
    try:
        conn = p.connect(dbname= ENV_PSQL_NAME, user=ENV_PSQL_USER, host=ENV_PSQL_HOST, password=ENV_PSQL_PASSWORD, port=ENV_PSQL_PORT)
        cursor = conn.cursor()
        cursor.execute(f"""
                       SELECT p.descripcion, p.codigo, p.id, sl.nombre, l.nombre, p.activo FROM {ENV_PSQL_DB_SCHEMA}.admintotal_producto as p 
                       INNER JOIN {ENV_PSQL_DB_SCHEMA}.admintotal_sublinea as sl ON p.sublinea_id = sl.id 
                       INNER JOIN {ENV_PSQL_DB_SCHEMA}.admintotal_linea as l ON p.linea_id = l.id""")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        conn = m.connect(host=ENV_MYSQL_HOST, user=ENV_MYSQL_USER, password=ENV_MYSQL_PASSWORD, database=ENV_MYSQL_NAME, port=ENV_MYSQL_PORT)
        cursor = conn.cursor()
        cursor.execute("SELECT id, code FROM product")
        rows2 = cursor.fetchall()
        for row in rows:
            if row[1] not in [r[1] for r in rows2]:
                cursor.execute(f"INSERT INTO product (description, code, id_admin, family_id, subfamily_id) VALUES ('{row[0]}', '{row[1]}', '{row[2]}', (SELECT id FROM family WHERE name = '{row[4]}'), (SELECT id FROM subfamily WHERE name = '{row[3]}'))")
            elif row[5] == True:
                cursor.execute(f"UPDATE product SET description = '{row[0]}', family_id = (SELECT id FROM family WHERE name = '{row[4]}'), subfamily_id = (SELECT id FROM subfamily WHERE name = '{row[3]}') WHERE code = '{row[1]}'")
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        if conn:
            conn.close()
        if cursor:
            cursor.close()



