import pandas as pd
from utils.db_connection import conect_bd
from utils.open_ai_api import aplicar_prompt


### Base de datos externa de los colegios a los que se les va a subir texto ###
df = pd.read_csv('')

environment = 'production' #staging production
tenant = 'chile'

promt = """
Genera un resumen en español de máximo 150 caracteres con las cosas más importantes de este texto y que este pensado en mejorar el seo de la página web del colegio. Evita cosas como "inscribete".   
"""
########################################################################################


conn_core = conect_bd('core', environment)

# Sports
consulta = f'''
    SELECT campus_code, sport_name_en
    FROM {tenant}.institutions_sport ids
    LEFT JOIN {tenant}.institutions_sport_label labels ON ids.sport_label_id=labels.id   
'''

sports = pd.read_sql(consulta, conn_core)
sports['campus_code'] = sports['campus_code'].astype(str)
sports = sports.groupby('campus_code')['sport_name_en'].apply(', '.join).reset_index()
sports['sport_name_en'] = "The school offers the following sports: " + sports['sport_name_en']

df = pd.merge(df, sports, on='campus_code', how='left')


# Infrastructure
consulta = f'''
    SELECT campus_code, infra_name_en
    FROM {tenant}.institutions_infrastructure ids
    LEFT JOIN {tenant}.institutions_infrastructure_label labels ON ids.infra_label_id=labels.id  
'''

infra = pd.read_sql(consulta, conn_core)
infra['campus_code'] = infra['campus_code'].astype(str)
infra = infra.groupby('campus_code')['infra_name_en'].apply(', '.join).reset_index()
infra['infra_name_en'] = "The school has the following infrastructure: " + infra['infra_name_en']

df = pd.merge(df, infra, on='campus_code', how='left')


# Extraactivity
consulta = f'''
    SELECT campus_code, extra_name_en
    FROM {tenant}.institutions_extraactivity ids
    LEFT JOIN {tenant}.institutions_extraactivity_label labels ON ids.extra_label_id=labels.id  
'''

extra = pd.read_sql(consulta, conn_core)
extra['campus_code'] = extra['campus_code'].astype(str)
extra = extra.groupby('campus_code')['extra_name_en'].apply(', '.join).reset_index()
extra['extra_name_en'] = "The school offers the following extraactivities: " + extra['extra_name_en']

df = pd.merge(df, extra, on='campus_code', how='left')


# Languages
consulta = f'''
    SELECT campus_code, language_name_en
    FROM {tenant}.institutions_language ids
    LEFT JOIN {tenant}.institutions_language_label labels ON ids.language_label_id=labels.id  
'''

languages = pd.read_sql(consulta, conn_core)
languages['campus_code'] = languages['campus_code'].astype(str)
languages = languages.groupby('campus_code')['language_name_en'].apply(', '.join).reset_index()
languages['language_name_en'] = "The school offers the following languages: " + languages['language_name_en']

df = pd.merge(df, languages, on='campus_code', how='left')

conn_core.close()





