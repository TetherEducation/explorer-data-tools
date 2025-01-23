import pandas as pd
from utils.db_connection import conect_bd
from utils.open_ai_api import aplicar_prompt


###Setear variables###
input_pat = "/Users/leidygomez/ConsiliumBots Dropbox/Leidy Gomez/Explorador_CB/Explorador_Chile/E_Escolar/outputs/update/2025_01_20/"
output_path = '/Users/leidygomez/ConsiliumBots Dropbox/Leidy Gomez/Explorador_CB/Explorador_Chile/E_Escolar/outputs/update/2025_01_20/'

environment = 'production' #staging production
tenant = 'chile'

######################


# Conectando a base de datos
conn_core = conect_bd('core', environment)

# Obtener tabla de institutions_campus_seo_meta para procesar solo los que estan vacios
consulta = f'''
    SELECT campus_code, description
    FROM {tenant}.institutions_campus_seo_meta
    WHERE description is null OR description=''
'''

df = pd.read_sql(consulta, conn_core)
df['campus_code'] = df['campus_code'].astype(str)


#agregando el nombre del colegio y el tipo 
consulta = f'''
    SELECT campus_code, campus_name, sector_name
    FROM {tenant}.institutions_campus campus
    LEFT JOIN {tenant}.institutions_sector_label sector ON campus.sector_label_id=sector.id
'''

campuses = pd.read_sql(consulta, conn_core)
df = pd.merge(df, campuses, on='campus_code', how='left')


#agregando ubicacion
consulta = f'''
    WITH config AS (
        SELECT
            'Provincia' AS geo_subd_mayor_label,
            'Comuna' AS geo_subd_menor_label,
            'Chile' AS pais
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
df = df.fillna("")

conn_core.close()


for index, row in df.iterrows():   
    parte1 = f"Explora el perfil digital de {row['campus_name']}"
    if row['sector_name']!='' and row['sector_name']!="Sin Información":
        parte2 = f" de caracter {row['sector_name']}"
    else: 
        parte2 = ""
    if row['geo_subd_menor_label']!='':
        parte3 = f" ubicado en la comuna {row['geo_subd_menor_label']}"
    else: 
        parte3 = ""
    if parte2=="" and parte3=="":
        parte4 = ". Facilitamos la búsqueda de colegios ideales."
    else:
        parte4 = ""
    
    df.at[index, 'description'] = parte1 + parte2 + parte3 + parte4

df = df[['campus_code', 'description']]
df.to_csv(output_path + 'summary_short_noAI.csv', sep=';', index=False)


















