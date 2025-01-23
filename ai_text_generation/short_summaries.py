import pandas as pd
from utils.db_connection import conect_bd
from utils.open_ai_api import aplicar_prompt


###Setear variables###
output_path = '/Users/leidygomez/ConsiliumBots Dropbox/Leidy Gomez/Explorador_CB/Explorador_Chile/E_Escolar/outputs/update/2025_01_20/'

environment = 'production' #staging production
tenant = 'chile'

promt = """
Genera un resumen en español de máximo 150 caracteres que use las palabras claves de este texto y que este pensado en mejorar el seo de la página web del colegio. Evita cosas como "inscribete".   
"""
######################

# Conectando a base de datos
conn_core = conect_bd('core', environment)

# Obtener tabla de institutions_text
consulta = f'''
    SELECT campus_code, profile_summary
    FROM {tenant}.institutions_text
    WHERE profile_summary is not null AND profile_summary!=''
'''

df = pd.read_sql(consulta, conn_core)
df = df[['campus_code', 'profile_summary']]
df['campus_code'] = df['campus_code'].astype(str)

#agregando el nombre del colegio
consulta = f'''
    SELECT campus_code, campus_name
    FROM {tenant}.institutions_campus
'''

campuses = pd.read_sql(consulta, conn_core)
conn_core.close()

df = pd.merge(df, campuses, on='campus_code', how='left')


for index, row in df.iterrows():
    campus_name = row['campus_name'] 
    text_all = row['profile_summary']
    input_text =  f"The following text contains information about the campus named '{campus_name}'. Please generate the short summary accordingly:\n\n{text_all}"

    print(f"Procesado índice {index}/{len(df)}")

    summaries_short = aplicar_prompt(input_text, promt)
    df.at[index, 'summaries_short'] = summaries_short   

df = df[['campus_code', 'summaries_short']]

df.to_csv(output_path + 'summary_short.csv', sep=';', index=False)





