import pandas as pd
import tiktoken
import os
from utils.scraper_very_detailed import crawl_and_accumulate_with_token_limit
from utils.db_connection import conect_bd
from utils.open_ai_api import aplicar_prompt


#######################Setear variables####################
input_path = '/Users/leidygomez/ConsiliumBots Dropbox/Leidy Gomez/Explorador_CB/Explorador_Copenhague/Outputs/'

output_path = '/Users/leidygomez/ConsiliumBots Dropbox/Leidy Gomez/Explorador_CB/Explorador_Copenhague/Inputs/text_generation_ai/'

os.makedirs(output_path + "pdfs", exist_ok=True)

gpt_model = "gpt-4o-mini"
environment = 'staging' #staging production
tenant = 'dnk'

prompt = """
You are an expert in summarizing information related to schools. You will be given a text with details about a school, and your task is to extract specific information related to the topics listed below. The output must be in JSON format, with each topic represented as a key and its respective summary as the value.
The summaries must be tailored for parents interested in the school. Do not add explanations like "it is mentioned" or "it is not mentioned." Use only the provided text to create the summaries.  If no relevant information is found for a topic, include the key with the value: “Topic is missing". For all topics, generate a summary in english of maximum 150 characters, unless explicitly instructed otherwise.
The topics you need to search for and summarize are:
1. Sports: Look for explicit mentions of sports in the text to generate the summary. For example, if the school offers soccer, basketball, or other sports, or highlight if sports are emphasized in their program.
2. Infrastructure: Look for details about the school's physical facilities. For example, facilities like a library, infirmary, store, science lab, sports courts, pool, theater, green areas, or any other relevant infrastructure.
3. Extracurricular Activities: Identify mentions of extracurricular activities. For instance, mention activities like dance, painting, poetry, yoga, college preparation, a band, astronomy club, or any other club or activity beyond the normal school schedule.
4. Languages: Look for mentions of foreign languages taught at the school.
5. Performance: Look for explicit mentions of academic performance or results in the text. For example, mention whether students have excelled in academic competitions or have obtained high scores on national tests. If the name of a specific competition or exam in which they have excelled and the dates are mentioned, add this information as well to be more detailed. Don't include anything about performance if the school only mentions that it has among its goals improving its academic performance; only include things that have already happened.
6. Teachers: Identify mentions related to teachers or the teaching staff in the text. Do not include numbers; focus on qualitative aspects related to the educators.
7. Students: Look for mentions of students or notable alumni. Avoid including numbers and focus on qualitative mentions related to students. If they mention an outstanding student, include the details.
8. Summary: Generate a general summary of the school in approximately 1300 characters, highlighting the most prominent elements of the institution.
9. Pride Points: The goal is to highlight the aspects the school focuses on the most. Identify the three themes that make the school stand out and unique. Some examples of standout categories could be: Comprehensive Development, Support for Special Educational Needs, Community Engagement, Academic Results, Teaching Team, Ethical/Religious Education, Learning Support Team, Technical/Vocational Education, Languages, Infrastructure, Arts and Culture, Higher Education Preparation, Extracurricular Activities, Sports, Elective Courses, Employment Placement, and Parent/Family Engagement. Select the three most notable elements and generate a title and a description of about 400 characters for each, formatted as follows:
1. <Title>: <Description>
2. <Title>: <Description>
3. <Title>: <Description>
JSON Structure: { "sports": "<summary>", "infrastructure": "<summary>", "extraactivities": "<summary>", "languages": "<summary>", "performance": "<summary>", "teacher": "<summary>", "students": "<summary>", "summary": "<detailed summary>", "pridepoints": { "1": { "title": "<Title>", "description": "<Description>" }, "2": { "title": "<Title>", "description": "<Description>" }, "3": { "title": "<Title>", "description": "<Description>" } } }. Please do not make up anything, use only the information in the text. If information about a topic is missing, use this format in the JSON output: "key": “Topic is missing"
"""

###########################################################

df = pd.read_csv(input_path + 'campuses_web_pages.csv')
df['campus_code'] = df['campus_code'].astype(str)

#Esto lo agregue despues para no repetir los que ya tienen textos
df2 = pd.read_csv(output_path + 'textos_openai_parte1.csv', sep=";")
df2['campus_code'] = df2['campus_code'].astype(str)

df = pd.merge(df, df2, on='campus_code', how='left')
df = df[df['summaries'].isna()]
df = df[['campus_code', 'webpage']].reset_index(drop=True)


conn_core = conect_bd('core', environment)
#agregando el nombre del colegio
consulta = f'''
    SELECT campus_code, campus_name
    FROM {tenant}.institutions_campus
'''

campuses = pd.read_sql(consulta, conn_core)
conn_core.close()

df = pd.merge(df, campuses, on='campus_code', how='left')


# Asegurar que todas las URLs tengan esquema (http/https)
def asegurar_esquema(url):
    if url != 'nan' and not url.startswith(('http://', 'https://')):
        return f'https://{url}'
    return url

df['webpage'] = df['webpage'].astype(str) 
df['web_adr'] = df['webpage'].apply(asegurar_esquema)

save_every = 20 

# Procesar todas las URLs en el DataFrame
for index, row in df.iterrows():
    contenido = crawl_and_accumulate_with_token_limit(row['web_adr'], output_path, gpt_model, max_pages=2000, max_tokens=120000)
    df.at[index, 'contenido'] = contenido   

    # Inicializa el tokenizer
    tokenizer = tiktoken.encoding_for_model(gpt_model)
    
    # Tokeniza el texto y cuenta los tokens
    tokens = tokenizer.encode(contenido)
    num_tokens = len(tokens)
    
    # Si el número de tokens supera el límite, recorta el texto
    if num_tokens > 128000:
        print(f"El texto del índice {index} supera el límite de 128,000 tokens. Se recortará.")
        # Decodifica únicamente los primeros 125,000 tokens para mantener el texto dentro del límite
        truncated_text = tokenizer.decode(tokens[:120000])
        contenido = truncated_text

    campus_name = row['campus_name'] 
    input_text =  f"The following text contains information about the campus named '{campus_name}'. Please generate the short summary accordingly:\n\n{contenido}"

    summaries = aplicar_prompt(input_text, prompt)
    df.at[index, 'summaries'] = summaries  

    print(f"Procesado openAI índice {index}/{len(df)}")

    # Guardar el progreso cada 'save_every' iteracciones
    if index % save_every == 0 or index == len(df) - 1:
        df.to_csv(output_path + 'textos_openai.csv', sep=';', index=False, columns=['campus_code', 'web_adr', 'summaries'])
        df.to_csv(output_path + 'textos_openai_con_input.csv', sep=';', index=False, columns=['campus_code', 'web_adr', 'contenido', 'summaries'])
        print(f"Progreso guardado. Iteración {index}/{len(df)}")



