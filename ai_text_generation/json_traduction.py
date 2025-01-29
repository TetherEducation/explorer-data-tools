import json
from utils.open_ai_api import aplicar_prompt

###Setear variables###
input_path = "/Users/leidygomez/Downloads/"

output_path = "/Users/leidygomez/Downloads/"

prompt = """
Translate the following text to Danish, keeping it concise and accurate:
"""
#######################


# Cargar el JSON original
with open(input_path + "en.json", "r", encoding="utf-8") as file:
    json_content = json.load(file)


# Función recursiva para traducir JSON
def translate_json(json_obj, target_language="Danish"):
    if isinstance(json_obj, dict):
        return {key: translate_json(value, target_language) for key, value in json_obj.items()}
    elif isinstance(json_obj, list):
        return [translate_json(item, target_language) for item in json_obj]
    elif isinstance(json_obj, str):
        return aplicar_prompt(json_obj, prompt)
    else:
        return json_obj

# Traducir el JSON completo
translated_json = translate_json(json_content)

# Guardar el JSON traducido
with open(output_path + "translated_json.json", "w", encoding="utf-8") as file:
    json.dump(translated_json, file, ensure_ascii=False, indent=2)

print("JSON traducido y guardado como 'translated_json.json'")
