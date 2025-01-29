
import pandas as pd
import uuid
from datetime import datetime
import psycopg2
from dotenv import load_dotenv
import os



###Definiendo variables iniciales###
pathData = '/Users/leidygomez/Desktop/cargas_directores/2024_11_27/'
pathCred = '/Users/leidygomez/Documents/cred'
environment = 'production'
tenant = 'chile'
date = datetime.now()



if tenant == 'chile':
    cel_code = '+56'
if tenant == 'colombia':
    cel_code = '+57'
if tenant == 'usa_ca':
    cel_code = '+1'

os.chdir(pathCred)
load_dotenv()


###Definiendo funciones###
#Funcion para conectarse a la base de datos
def conect_bd(database, environment):
    if environment == 'staging':
        host = "staging.cluster-cuasuehzhaay.us-east-1.rds.amazonaws.com"
    elif environment == 'production':
        host = "production.cluster-cuasuehzhaay.us-east-1.rds.amazonaws.com"  
    conn = psycopg2.connect(
        database = database,
        user = os.getenv("db_user"),
        password = os.getenv("db_password"),
        port = os.getenv("db_port"),
        host = host
    )
    return conn


#Funcion para conectarse a la base de datos
def conect_bd_users_prod(database):
    conn = psycopg2.connect(
        database = database,
        user = os.getenv("users_user"),
        password = os.getenv("users_password"),
        port = "5432",
        host = "users.cluster-cuasuehzhaay.us-east-1.rds.amazonaws.com"
    )
    return conn



###Generando conexion a la base de datos de users###
if environment == 'staging':
    conn_user = conect_bd('users', environment) 
elif environment == 'production':
    conn_user = conect_bd_users_prod('users') 

###Generando conexion a la base de datos core###
conn_core = conect_bd('core', environment)





##Abrir base de datos externa
datos = pd.read_excel(pathData + 'Explorador.xlsx', engine='openpyxl')
datos['username'] = datos['CORREO'].str.strip().str.lower() 

datos['name'] = datos['DIRECTOR/A'].str.split().str[0].str.title()

datos = datos.rename(columns={'RBD': 'institution_code'})
datos['institution_code'] = datos['institution_code'].astype(str)


query_campuses = f"""
    SELECT campus_code, institution_code
    FROM {tenant}.institutions_campus
"""
campuses = pd.read_sql(query_campuses, conn_core)
campuses[['campus_code', 'institution_code']] = campuses[['campus_code', 'institution_code']].astype(str)

datos = pd.merge(datos, campuses, on='institution_code', how='left')

datos = datos[['institution_code', 'campus_code', 'username', 'name']]




#Procesando tabla users
users = datos.drop_duplicates(subset=['username'])
list_of_users = list(users['username'])

users = datos.drop_duplicates(subset=['username'])
list_of_users = list(datos['username'])


###Se contrasta la lista de usernames con los que existen en base de datos###
consulta = f"SELECT * FROM PUBLIC.cb_user WHERE username IN ({list_of_users})"
consulta = consulta.replace("[", "").replace("]", "")
users_existen = pd.read_sql(consulta, conn_user)

users_new = pd.merge(users, users_existen, on=['username'], how='left',  indicator=True)
users_new = users_new[users_new['_merge'] == 'left_only'].drop(columns=['_merge'])
users_new = users_new[['username', 'name']]

users_new['id'] = users_new.index.map(lambda x: uuid.uuid4()).map(str)
users_new['password'] = 'pbkdf2_sha256$600000$oCmPzioGAIBDi9I3ZBLFDU$Fv4De8MD7k0mIgiMYX9X1J2dBiaNgTRlScgHSkJ7Jc8=' ## el password es Provisoria123
users_new['first_name'] = users_new['name']
users_new['last_name'] = ''
users_new['is_active'] = 'True'
users_new['is_verified'] = 'True'
users_new['email'] = users_new['username']



users_columns = ['id', 'username', 'password', 'first_name', 'last_name', 'email',  'is_active', 'is_verified']

users_new = users_new[users_columns]


# Convertir los datos a una lista de tuplas
data_to_insert = [tuple(x) for x in users_new.itertuples(index=False, name=None)]

# Comando de inserción masiva
insert_query = """
    INSERT INTO PUBLIC.cb_user (id, username, password, first_name, last_name, email, is_active, is_verified)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""


# Ejecutar la inserción masiva
try:
    with conn_user.cursor() as cursor:
        cursor.executemany(insert_query, data_to_insert)
    conn_user.commit()  # Hacer commit para confirmar las inserciones
except Exception as e:
    conn_user.rollback()  # Deshacer la transacción en caso de error
    print(f"Error: {e}")


conn_user.close()





###Procesando tabla de legal guardian

#Revisando que los que ya estaban en user si esten en legal guardian
users_new = users_new.rename(columns={'id': 'user'})
list_of_users_existen = list(users_existen['id'])
consulta = f'SELECT "user", id as legalguardian_id FROM {tenant}.registration_legalguardian WHERE "user" IN ({list_of_users_existen})'
consulta = consulta.replace("[", "").replace("]", "")

existen = pd.read_sql(consulta, conn_core)


users_existen = users_existen.rename(columns={'id': 'user'})
lg_new = pd.merge(users_existen, existen, on=['user'], how='left',  indicator=True)
lg_new = lg_new[lg_new['_merge'] == 'left_only'].drop(columns=['_merge'])

#A los que faltan en la tabla y a los nuevos se les inserta
lg_new = pd.concat([lg_new, users_new], ignore_index=True)


lg_new = lg_new[['user', 'email', 'first_name']]
lg_new['uuid'] = lg_new.index.map(lambda x: uuid.uuid4()).map(str)
lg_new['created'] = date
lg_new['modified'] = date 
lg_new['registered'] = 'True'
lg_new['first_lastname'] = ''
lg_new['register_step'] =  0
lg_new['failed_login_attempts'] = 0
lg_new['is_verified'] = 'True'
lg_new['first_login'] = 'False'
lg_new['contact_preference_call'] = 'True'
lg_new['contact_preference_whatsapp'] = 'True'
lg_new['contact_preference_sms'] = 'True'
lg_new['contact_preference_email'] = 'True'
lg_new['notify_importantdates'] = 'True'
lg_new['notify_interestschools'] = 'True'
lg_new['contact_preference_sms'] = 'True'
lg_new['contact_preference_email'] = 'True'
lg_new['notify_important_dates'] = 'True'
lg_new['notify_interest_schools'] = 'True'


lg_columns = ['created', 'modified', 'uuid', 'registered', 'first_name', 'first_lastname',  'email', 'register_step', 'failed_login_attempts', 'is_verified', 'user', 'first_login', 'contact_preference_call', 'contact_preference_whatsapp', 'contact_preference_sms', 'contact_preference_email', 'notify_importantdates', 'notify_interestschools', 'notify_important_dates', 'notify_interest_schools']

lg_new = lg_new[lg_columns]
lg_new = lg_new.where(pd.notnull(lg_new), None)

# Convertir los datos a una lista de tuplas
data_to_insert = [tuple(x) for x in lg_new.itertuples(index=False, name=None)]


# Comando de inserción masiva
insert_query = f"""
    INSERT into {tenant}.registration_legalguardian  (created, modified, uuid, registered, first_name, first_lastname, email, register_step, failed_login_attempts, is_verified, "user", first_login, contact_preference_call, contact_preference_whatsapp, contact_preference_sms, contact_preference_email, notify_importantdates, notify_interestschools, notify_important_dates, notify_interest_schools)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Ejecutar la inserción masiva
try:
    with conn_core.cursor() as cursor:
        cursor.executemany(insert_query, data_to_insert)
    conn_core.commit()  # Hacer commit para confirmar las inserciones
except Exception as e:
    conn_core.rollback()  # Deshacer la transacción en caso de error
    print(f"Error: {e}")




###Procesando application_system_user_roles
consulta = f'SELECT "user", role_id FROM {tenant}.application_system_user_roles WHERE "user" IN ({list_of_users_existen})'
consulta = consulta.replace("[", "").replace("]", "")

existen = pd.read_sql(consulta, conn_core)

role_new = pd.merge(users_existen, existen, on=['user'], how='left',  indicator=True)
role_new = role_new[role_new['_merge'] == 'left_only'].drop(columns=['_merge'])

#A los que faltan en la tabla y a los nuevos se les inserta
role_new = pd.concat([role_new, users_new])

role_new['created'] = date
role_new['modified'] = date 
role_new['role_id'] = 3 

role_columns = ['created', 'modified','user', 'role_id']

role_new = role_new[role_columns]
role_new = role_new.where(pd.notnull(role_new), None)

# Convertir los datos a una lista de tuplas
data_to_insert = [tuple(x) for x in role_new.itertuples(index=False, name=None)]


# Comando de inserción masiva
insert_query = f"""
    INSERT into {tenant}.application_system_user_roles (created, modified, "user", role_id)
    VALUES (%s, %s, %s, %s)
"""

# Ejecutar la inserción masiva
try:
    with conn_core.cursor() as cursor:
        cursor.executemany(insert_query, data_to_insert)
    conn_core.commit()  # Hacer commit para confirmar las inserciones
except Exception as e:
    conn_core.rollback()  # Deshacer la transacción en caso de error
    print(f"Error: {e}")




###Procesando application_system_user_campus
users_existen = users_existen[['user', 'username']]
users_existen.to_csv(pathData + 'cuentas_existen.csv', index=False)
users_new = users_new[['user', 'username']]
users_new.to_csv(pathData + 'cuentas_nuevas.csv', index=False)
users_all = pd.concat([users_existen, users_new], ignore_index=True)
datos = pd.merge(datos, users_all, on='username', how='left')
datos = datos[['user', 'campus_code', 'institution_code']]


list_of_campuses = list(datos['campus_code'])
list_of_users = list(datos['user'])
consulta = f'SELECT "user", campus_code  FROM {tenant}.application_system_user_campus WHERE "user" IN ({list_of_users}) AND campus_code IN ({list_of_campuses})'
consulta = consulta.replace("[", "").replace("]", "")

existen = pd.read_sql(consulta, conn_core)

user_campus_new = pd.merge(datos, existen, on=['user', 'campus_code'], how='left',  indicator=True)
user_campus_new = user_campus_new[user_campus_new['_merge'] == 'left_only'].drop(columns=['_merge'])

user_campus_new['enabled'] = True

user_campus_columns = ['user', 'enabled', 'campus_code', 'institution_code']

user_campus_new = user_campus_new[user_campus_columns]
user_campus_new = user_campus_new.where(pd.notnull(user_campus_new), None)

# Convertir los datos a una lista de tuplas
data_to_insert = [tuple(x) for x in user_campus_new.itertuples(index=False, name=None)]


# Comando de inserción masiva
insert_query = f"""
    INSERT into {tenant}.application_system_user_campus ("user", enabled, campus_code, institution_code)
    VALUES (%s, %s, %s, %s)
"""

# Ejecutar la inserción masiva
try:
    with conn_core.cursor() as cursor:
        cursor.executemany(insert_query, data_to_insert)
    conn_core.commit()  # Hacer commit para confirmar las inserciones
except Exception as e:
    conn_core.rollback()  # Deshacer la transacción en caso de error
    print(f"Error: {e}")




conn_core.close()



