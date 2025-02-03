import pandas as pd
from utils.db_connection import conect_bd, conect_bd_users_prod
import uuid
from datetime import datetime


###Setear variables###
users_to_create = 30

intervention_label_id = 3  #Esto sale de la tabla reports_intervention_label
treatment_id = 2

environment = 'production'
tenant = 'newhaven'
date = datetime.now()

output_path = "/Users/leidygomez/Downloads/usuarios_fake_Nh/"
######################



###Generando conexion a la base de datos de users###
if environment == 'staging':
    conn_user = conect_bd('users', environment) 
elif environment == 'production':
    conn_user = conect_bd_users_prod('users') 


#Generando emails falsos
num_observaciones = users_to_create
# Generar datos
data = {
    "username": [f"user_intervention_prod{i}@yopmail.com" for i in range(1, num_observaciones + 1)],
    "email": [f"user_intervention_prod{i}@yopmail.com" for i in range(1, num_observaciones + 1)],
}
# Crear el DataFrame
users_new = pd.DataFrame(data)

users_new['id'] = users_new.index.map(lambda x: uuid.uuid4()).map(str)
users_new['password'] = 'pbkdf2_sha256$600000$21adrfr6nZmslzY5Uib5oK$QX7Yf81MXbDkkSwJuM5fiswme10z23GRPBbVJmNtzLQ='
users_new['first_name'] = 'Legal guardian'
users_new['last_name'] = 'Guardian'
users_new['is_active'] = 'True'
users_new['is_verified'] = 'True'



users_columns = ['id', 'username', 'password', 'first_name', 'last_name', 'email',  'is_active', 'is_verified']

users_new = users_new[users_columns]
#users_new = users_new.where(pd.notnull(users_new), None)


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



#Cerrando conexion
conn_user.close()

users_new.to_csv(output_path + f'users_fake_nh_{environment}.csv', index=False)






###Generando conexion a la base de datos core###
conn_core = conect_bd('core', environment)



#Tabla de application_system_user_roles
role_new = users_new[['id']]
role_new = role_new.rename(columns=({'id': 'user'}))

role_new['created'] = date
role_new['modified'] = date 
role_new['role_id'] = 2 

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





###Tabla de registration_legalguardian###
lg_new = users_new[['id', 'email']]
lg_new = lg_new.rename(columns=({'id': 'user'}))
lg_new['uuid'] = lg_new.index.map(lambda x: uuid.uuid4()).map(str)
lg_new['created'] = date
lg_new['modified'] = date 
lg_new['registered'] = 'True'
lg_new['first_name'] = 'Guardian'
lg_new['first_lastname'] = 'Guardian'
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





###Tabla de registration_applicant###

#Crear el applicant id considerando el maximo id de la tabla
consult_query = f"""
    SELECT MAX(id)
    FROM {tenant}.registration_applicant
"""

# Ejecutar la consulta
max_app_id = pd.read_sql(consult_query, conn_core)
max_id = max_app_id.iloc[0, 0]



app_new = users_new[['id', 'email']]
app_new = app_new.rename(columns=({'id': 'user'}))
app_new['uuid'] = app_new.index.map(lambda x: uuid.uuid4()).map(str)
app_new['created'] = date
app_new['modified'] = date 
app_new['id'] = range(1, len(app_new) + 1) + max_id
app_new['registered'] = 'True'
app_new['first_name'] = 'Guardian'


app_columns = ['created', 'modified', 'uuid', 'id', 'registered', 'first_name', 'user']

app_new = app_new[app_columns]
app_new = app_new.where(pd.notnull(app_new), None)


# Convertir los datos a una lista de tuplas
data_to_insert = [tuple(x) for x in app_new.itertuples(index=False, name=None)]


# Comando de inserción masiva
insert_query = f"""
    INSERT into {tenant}.registration_applicant  (created, modified, uuid, id, registered, first_name, "user")
    VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

# Ejecutar la inserción masiva
try:
    with conn_core.cursor() as cursor:
        cursor.executemany(insert_query, data_to_insert)
    conn_core.commit()  # Hacer commit para confirmar las inserciones
except Exception as e:
    conn_core.rollback()  # Deshacer la transacción en caso de error
    print(f"Error: {e}")





###Tabla de reports_intervention###
reports_intervention = users_new[['id', 'email']]
reports_intervention = reports_intervention.rename(columns=({'id': 'user'}))
reports_intervention['uuid'] = reports_intervention.index.map(lambda x: uuid.uuid4()).map(str)
reports_intervention['created'] = date
reports_intervention['modified'] = date
reports_intervention['organic'] = 'False'  
reports_intervention['guest'] = 'False'  
reports_intervention['intervention_label_id'] = intervention_label_id
reports_intervention['treatment_id'] = treatment_id

reports_intervention_columns = ['created', 'modified', 'uuid', 'user', 'organic', 'guest', 'intervention_label_id', 'treatment_id']

reports_intervention = reports_intervention[reports_intervention_columns]
reports_intervention = reports_intervention.where(pd.notnull(reports_intervention), None)

# Convertir los datos a una lista de tuplas
data_to_insert = [tuple(x) for x in reports_intervention.itertuples(index=False, name=None)]


# Comando de inserción masiva
insert_query = f"""
    INSERT into {tenant}.reports_intervention  (created, modified, uuid, "user", organic, guest, intervention_label_id, treatment_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

# Ejecutar la inserción masiva
try:
    with conn_core.cursor() as cursor:
        cursor.executemany(insert_query, data_to_insert)
    conn_core.commit()  # Hacer commit para confirmar las inserciones
except Exception as e:
    conn_core.rollback()  # Deshacer la transacción en caso de error
    print(f"Error: {e}")


reports_intervention.to_csv(output_path + f'interventions_fake_nh_{environment}.csv', index=False)

uuids = reports_intervention[['uuid']]
uuids.to_csv(output_path + f'uuids_intervention_{environment}.csv', index=False)



###Tabla de reports_intervention_applicant###
intervention_applicant = reports_intervention[['uuid', 'user']]
intervention_applicant = intervention_applicant.rename(columns=({'uuid': 'intervention_id'}))
intervention_applicant = pd.merge(intervention_applicant, app_new, on=['user'], how='left')
intervention_applicant = intervention_applicant.rename(columns=({'uuid': 'applicant_uuid'}))
intervention_applicant['uuid'] = intervention_applicant.index.map(lambda x: uuid.uuid4()).map(str)

intervention_applicant_columns = ['uuid', 'applicant_uuid', 'intervention_id']

intervention_applicant = intervention_applicant[intervention_applicant_columns]
intervention_applicant = intervention_applicant.where(pd.notnull(intervention_applicant), None)



# Convertir los datos a una lista de tuplas
data_to_insert = [tuple(x) for x in intervention_applicant.itertuples(index=False, name=None)]


# Comando de inserción masiva
insert_query = f"""
    INSERT into {tenant}.reports_intervention_applicant  (uuid, applicant_uuid, intervention_id)
    VALUES (%s, %s, %s)
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










