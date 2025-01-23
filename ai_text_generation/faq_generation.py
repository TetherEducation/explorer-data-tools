import pandas as pd
import os
import tiktoken
from utils.db_connection import conect_bd
from utils.open_ai_api import aplicar_prompt
from utils.detailed_scrapper import crawl_and_extract

#######################Setear variables####################

gpt_model = "gpt-4o-mini"

pathCred = '/Users/leidygomez/Documents/cred'
os.chdir(pathCred)

output_path = '/Users/leidygomez/Documents/cred/output'
n_max = 1667 #Numero de archivos en la carpeta

# URL que se va a scrappear
base_url = "https://www.nhps.net/"

#Promt que se usa para generar las preguntas por cada fuente
promt1 = """
You are an expert summarizing information for a centralized admission process to public schools in New Haven. Please generate questions and answers using only the following text. Consider that each question will be read independently so please include all the important details in each question. Generate as many questions and answers as possible. For each question, associate it with one of the following categories: "admission process", "prices", "dates", "teachers", "students", "sports", "extracurriculars", "infrastructure", "languages", "educational-focus", "performance", "programs", "contact information", or "other" if it does not fit any of these categories. Return the result in a structured JSON format as follows: [ {"question": "<insert question>", "answer": "<insert answer>", "category": "<insert category>"}, {"question": "<insert question>", "answer": "<insert answer>", "category": "<insert category>"}, ... ]
"""

###############################################################################################

# # Crear un directorio para guardar los archivos
# os.makedirs(output_path, exist_ok=True)
# os.makedirs(output_path + "/pdfs", exist_ok=True)
# os.makedirs(output_path + "/html_texts", exist_ok=True)

# #scrappeo de la pagina web
# crawl_and_extract(base_url, output_path, max_pages=3)

# Ruta de la carpeta donde están los archivos
folder_path = f"{output_path}/html_texts"

# Crear una lista para almacenar los datos
data = []
errores = [] 

for i in range(1042,n_max):
    filename = f"link_{i}.txt"
    file_path = os.path.join(folder_path, filename)  # Ruta completa al archivo
    print(file_path)
    
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            input_text = file.read()
        
        input_text = "" if len(input_text) < 400 else input_text
        
        # Inicializa el tokenizer
        tokenizer = tiktoken.encoding_for_model(gpt_model)
        
        # Tokeniza el texto y cuenta los tokens
        tokens = tokenizer.encode(input_text)
        num_tokens = len(tokens)
        
        # Si el número de tokens supera el límite, recorta el texto
        if num_tokens > 128000:
           print("El texto supera el límite de 128,000 tokens. Se recortará.")
           # Decodifica únicamente los primeros 125,000 tokens para mantener el texto dentro del límite
           truncated_text = tokenizer.decode(tokens[:125000])
           input_text = truncated_text
        
        if input_text:
            faq = aplicar_prompt(input_text, promt1)
        else:
            faq = ""
            
        data.append({"filename": filename, "faq": faq})
    
    except Exception as e:
        print(f"Error al procesar el archivo {filename}: {e}")
        errores.append({"filename": filename, "error": str(e)})

df = pd.DataFrame(data)

df.to_csv("output/html_texts_contents_1042_resto.csv", index=False, encoding="utf-8")

