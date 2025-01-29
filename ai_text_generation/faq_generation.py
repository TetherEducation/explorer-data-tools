import pandas as pd
from utils.scraper_single_link import extract_text, clean_text
from utils.open_ai_api import aplicar_prompt

#######################Setear variables####################
output_path = "/Users/leidygomez/ConsiliumBots Dropbox/Leidy Gomez/Explorador_CB/Explorador_Connecticut/E_Escolar/outputs/update/2025_01_19"

promt1 = """
You are an expert summarizing information for a centralized admission process to public schools in New Haven. 
Please generate questions and answers using only the following text. Consider that each question will be read independently so please include all the important details in each question. 
Generate as many questions and answers as possible. For each question, associate it with one of the following categories: "admission process", "prices", "dates", "teachers", "students", "sports", "extracurriculars", "infrastructure", "languages", "educational-focus", "performance", "programs", "contact information", or "other" if it does not fit any of these categories. 
Return the result in a structured JSON format as follows: [ {"question": "<insert question>", "answer": "<insert answer>", "category": "<insert category>"}, {"question": "<insert question>", "answer": "<insert answer>", "category": "<insert category>"}, ... ]
"""

promt2 = """
In the following text there are 20 questions with answers. Please identify each question and its answer. For each question, associate it with one of the following categories: "admission process", "prices", "dates", "teachers", "students", "sports", "extracurriculars", "infrastructure", "languages", "educational-focus", "performance", "programs", "contact information", or "other" if it does not fit any of these categories. 
Return the result in a structured JSON format as follows: [ {"question": "<insert question>", "answer": "<insert answer>", "category": "<insert category>"}, {"question": "<insert question>", "answer": "<insert answer>", "category": "<insert category>"}, ... ]
"""

promt3 = """
The following text has information about 35 schools in New Haven with important dates. Can you generate a frequency asked question for each school with the information with the following structure: [ {"question": "<insert question>", "answer": "<insert answer>", "category": "<insert category>"}, {"question": "<insert question>", "answer": "<insert answer>", "category": "<insert category>"}, ... ]. Category is always "dates".
"""

links_importantes = [
    'https://www.newhavenmagnetschools.com/index.php/whats-the-process/the-application-process', 
    'https://www.newhavenmagnetschools.com/index.php/whats-the-process/neighborhood-sibling-preferences', 
    'https://www.newhavenmagnetschools.com/index.php/whats-the-process/open-house-dates', 
    'https://www.newhavenmagnetschools.com/index.php/whats-the-process/next-steps', 
    'https://www.newhavenmagnetschools.com/index.php/faq',
    'https://www.newhavenmagnetschools.com/',
    'https://www.newhavenmagnetschools.com/index.php/get-started/we-offer',
    'https://www.newhavenmagnetschools.com/index.php/about-the-schools/types-of-schools-to-choose-from',
    'https://newhavenmagnetschools.com/index.php/about-the-schools/about-pre-k-programs',
    'https://www.newhavenmagnetschools.com/index.php/about-the-schools/special-education',
    'https://www.newhavenmagnetschools.com/index.php/about-the-schools/english-learners-programs',
    'https://www.newhavenmagnetschools.com/index.php/about-the-schools/transportation'
    ]

###############################################################################################

#Extraer texto de los links
data=[]
for link in links_importantes:
    page_text = extract_text(link)
    cleaned_page_text = clean_text(page_text)
    data.append({"link": link, 
                 "text": cleaned_page_text})
    
df = pd.DataFrame(data) 

#Pedirle a openAI que genere faqs para cada
data=[]
for index, row in df.iterrows():
    text = row['text']
    link = row['link']
    if link=='https://www.newhavenmagnetschools.com/index.php/faq':
        faq = aplicar_prompt(text,promt2)
    elif link=='https://www.newhavenmagnetschools.com/index.php/whats-the-process/open-house-dates':
        faq = aplicar_prompt(text,promt3)
    else:
        faq = aplicar_prompt(text,promt1)
    data.append({"link": link, "faq": faq})

df.to_csv(output_path + "faq_links_principales_openai.csv", index=False, encoding="utf-8")


#Postprocesamiento

df = pd.read_csv(output_path + "/faq_links_principales_openai.csv")
df = df.fillna("")

import json

# Función para limpiar y cargar el JSON
def process_json_column(json_string):
    try:
        # Eliminar el prefijo y sufijo ```json y ``` si existen
        clean_json = json_string.strip("```json").strip("```").strip()
        
        # Convertir a JSON válido
        return json.loads(clean_json)
    except json.JSONDecodeError as e:
        print(f"Error al decodificar JSON: {e}")
        return []  # Devolver lista vacía en caso de error

# Expandir la columna `faq` y rastrear los links
expanded_data = []
links_without_questions = []  # Lista para rastrear links sin preguntas

for _, row in df.iterrows():
    link = row["link"]  # Link original
    json_list = process_json_column(row["faq"])  # Procesar y cargar JSON
    
    if not json_list:  # Si no hay preguntas, registrar el link
        links_without_questions.append(filename)
    else:
        for item in json_list:
            expanded_data.append({
                "link": link,
                "question": item.get("question"),
                "answer": item.get("answer"),
                "category": item.get("category")
            })

# Crear el DataFrame expandido
expanded_df = pd.DataFrame(expanded_data)

df=expanded_df


#Separo por los que hay que revisar
dates = df[df['category']=='dates']
dates.to_csv(output_path + "/dates.csv")

contact_info = df[df['category']=='contact information']
contact_info.to_csv(output_path + "/contact_info.csv")




promt4 = """
You will receive a question and an answer related to the admission process of New Haven public schools. Can you please identify if the question or answers mentions a specific school from the following list, if the name of the school appears in the list, please return de code from the dictionary. Return only the numeric code. If there is no mention of a school please return "No code".
shcools= {
    'Clinton Avenue School': 930611,
    'Common Ground Charter': 2686113,
    'Cooperative Arts & Humanities': 936411,
    'Elm City College Preparatory Charter Elementary School': 9351211,
    'Elm City College Preparatory Charter Middle School': 9351212,
    'Fair Haven School': 931611,
    'Highville Charter School and Change Academy': 2860113,
    'Hill Central School': 930711,
    'Hill Regional Career Interdistrict Magnet': 936311,
    'Engineering & Science University Magnet School': 931711,
    'East Rock Community Magnet': 934611,
    'Brennan-Rogers: The Art Of Communication & Media Magnet': 932111,
    'Celentano Biotech Health & Medical Magnet': 934811,
    'James Hillhouse Comprehensive High School': 936211,
    'John C. Daniels School Of International Communication Interdistrict Magnet': 931311,
    'L.W. Beecher Museum School Of Arts & Sciences Interdistrict Magnet': 930311,
    'Davis Academy for Arts & Design Innovation Magnet School': 930911,
    'High School In The Community; Interdistrict Magnet school for Leadership and Social Justice.': 936611,
    'Nathan Hale School': 931411,
    'Amistad Academy Charter Elementary & Middle School': 27951131,
    'Barack H. Obama Magnet University School': 932811,
    'Betsy Ross Arts Interdistrict Magnet': 935511,
    'Augusta Lewis Troup School': 931511,
    'Amistad Academy High School': 27951132,
    'Benjamin Jepson Multi-Age Interdistrict Magnet': 931811,
    'Worthington Hooker School (K-2nd)': 9338111,
    'Metropolitan Business Academy Interdistrict Magnet': 936011,
    'Lincoln-Bassett Community School': 932011,
    'John S. Martinez Sea & Sky STEM Magnet School': 930811,
    'Barnard Environmental Studies Interdistrict Magnet': 930211,
    'Bishop Woods Executive Academy': 934311,
    'Edgewood Magnet': 931211,
    'New Haven Academy Interdistrict Magnet': 937011,
    'Roberto Clemente Leadership Academy': 934211,
    'Truman School': 932911,
    'Wilbur L. Cross High School': 936111,
    'Ross Woodward Classical Studies Interdistrict Magnet': 931011,
    'Wintergreen Interdistrict Magnet': 2440314,
    'ACES Educational Center for the Arts': 2449900,
    'Booker T. Washington Academy Charter Middle School': 29501132,
    'The Sound School Regional Vocational Aquaculture Center': 936711,
    'Wexler-Grant Community School': 933211,
    'Elm City Montessori School': 2910113,
    'Family Academy of Multilingual Education': 934111,
    'Worthington Hooker School 3rd-8th': 9338112,
    'Booker T. Washington Academy Charter Elementary School': 29501131,
    'King/Robinson Interdistrict Magnet: An International Baccalaureate STEM School': 933011,
    'Harry A. Conte-West Hills Magnet: A School Of Exploration & Innovation': 933111,
    'Mauro-Sheridan Science, Technology & Communications Interdistrict Magnet': 931911,
    'Edmonds Cofield Preparatory Academy for Young Men': 1010101
    }
"""

for index, row in df.iterrows():
    print(index)
    question = row['question']
    answer = row['answer']
    faq =  f"The question is {question} and the answer is {answer}"
    
    campus_code = aplicar_prompt(faq, promt4)
    df.at[index, 'campus_code'] = campus_code 


df["campus_code"] = df["campus_code"].replace("No code", "")

section_map={
    "admission process": 4,
    "prices": 5,
    "teachers": 8,
    "students": 9,
    "programs": 15,
    "contact information": 1,
    "educational-focus": 2,
    "infrastructure": 10,
    "transportation": 3,
    "languages": 13
    }

df['section_label_id'] = df["category"].map(section_map)
df["section_label_id"] = df["section_label_id"].apply(lambda x: str(int(x)) if x.is_integer() else x)

df['question_es'] = df['question']
df['question_en'] = df['question']
df['answer_es'] = df['answer']
df['answer_en'] = df['answer']
df['process_id'] = 36
df['order'] = range(1, len(df) + 1)

df = df[['order', 'question', 'answer','question_en', 'answer_en','question_es', 'answer_es', 'process_id', 'campus_code', 'section_label_id']]
df = df.fillna("")

df.to_csv(output_path + "/institutions_frequently_asked_question.csv", index=False, float_format="%.0f", encoding='utf-8')


