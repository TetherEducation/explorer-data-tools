import pandas as pd
from utils.db_connection import conect_bd
from utils.open_ai_api import aplicar_prompt


###Setear variables###
input_pat = "/Users/leidygomez/ConsiliumBots Dropbox/Leidy Gomez/Explorador_CB/Explorador_Chile/E_Escolar/outputs/update/2025_01_20/"
output_path = '/Users/leidygomez/ConsiliumBots Dropbox/Leidy Gomez/Explorador_CB/Explorador_Chile/E_Escolar/outputs/update/2025_01_20/'

environment = 'production' #staging production
tenant = 'Dom'

######################

# Conectando a base de datos
conn_core = conect_bd('core', environment)

#obtener tabla de campus_code
consulta = f'''
    SELECT campus_code, campus_name, sector_name
    FROM {tenant}.institutions_campus campus
    LEFT JOIN {tenant}.institutions_sector_label sector ON campus.sector_label_id=sector.id
'''
df = pd.read_sql(consulta, conn_core)
df['campus_code'] = df['campus_code'].astype(str)

#agregando ubicacion
consulta = f'''
    WITH config AS (
        SELECT
            'Municipio' AS geo_subd_mayor_label,
            'Distrito Municipal' AS geo_subd_menor_label,
            'Dom' AS pais
    ),
    location AS (
        SELECT campus_code, latitud, longitud, address_street, plocation_id
        FROM {tenant}.institutions_location
    ),
    geo_subd_menor AS (
        SELECT menor.name, menor.id, menor.upper_location_id
        FROM {tenant}.places_location menor
        CROSS JOIN config
        WHERE menor.label_subdivision = config.geo_subd_menor_label
    ),
    geo_subd_mayor AS (
        SELECT mayor.name, mayor.id
        FROM {tenant}.places_location mayor
        CROSS JOIN config
        WHERE mayor.label_subdivision = config.geo_subd_mayor_label
    )
    SELECT campus.campus_code AS campus_code, location.address_street AS direccion, geo_subd_menor.name AS geo_subd_menor_label, geo_subd_mayor.name AS geo_subd_mayor_label 
    FROM {tenant}.institutions_campus campus
    CROSS JOIN config
    LEFT JOIN location ON campus.campus_code = location.campus_code
    LEFT JOIN geo_subd_menor ON location.plocation_id = geo_subd_menor.id
    LEFT JOIN geo_subd_mayor ON geo_subd_menor.upper_location_id = geo_subd_mayor.id
'''

locations = pd.read_sql(consulta, conn_core)
df = pd.merge(df, locations, on='campus_code', how='left')

#Agregando paginas web
consulta = f'''
    SELECT campus_code, webpage, facebook, instagram, youtube, twitter
    FROM {tenant}.institutions_social_networks
'''

social_networks = pd.read_sql(consulta, conn_core)
df = pd.merge(df, social_networks, on='campus_code', how='left')
df = df.fillna("")
print(df.head())

conn_core.close()


#Colombia

# df = df[(df['geo_subd_menor_label']=='Cali') | (df['geo_subd_menor_label']=='Medellín') | (df['geo_subd_menor_label']=='Envigado')]
# df = df[(df['sector_name']=='Privado')]
# df = df.sort_values(['geo_subd_menor_label'])
# df = df.rename(columns={'geo_subd_menor_label': 'Municipio', 'geo_subd_mayor_label': 'Departamento'})
# df.to_csv('/Users/leidygomez/Downloads/hunting_Medellín_Cali_privados.csv', index=False)


# #Peru

# df = df[(df['geo_subd_menor_label']=='LIMA') | (df['geo_subd_menor_label']=='TACNA')]
# df = df[(df['sector_name']=='Privada') | (df['sector_name']=='Pública de gestión privada')]
# df = df.sort_values(['geo_subd_menor_label'])
# df = df.rename(columns={'geo_subd_menor_label': 'Distrito', 'geo_subd_mayor_label': 'Provincia'})
# df.to_csv('/Users/leidygomez/Downloads/hunting_Lima_Tacna_privados.csv', index=False)


# #Dominicana

# df = df[(df['geo_subd_menor_label']=='Santiago') | (df['geo_subd_menor_label']=='Santo Domingo Norte') | (df['geo_subd_menor_label']=='Santo Domingo Este') | (df['geo_subd_menor_label']=='Santo Domingo Oeste')]
# df = df[(df['sector_name']=='Privado')]
# df = df.sort_values(['geo_subd_menor_label'])
# df = df.rename(columns={'geo_subd_menor_label': 'Distrito Municipal', 'geo_subd_mayor_label': 'Municipio'})
# df.to_csv('/Users/leidygomez/Downloads/hunting_Santiago_StoDomingo_privados.csv', index=False)








