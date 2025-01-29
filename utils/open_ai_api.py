from dotenv import load_dotenv
load_dotenv()
import openai
import os

gpt_model = "gpt-4o-mini"
openai.api_key = os.getenv("openai_key")

def aplicar_prompt(texto, prompt):
    """
    Utilizes the OpenAI API to process text with a given prompt.
    """
    try:
        # Formatear la solicitud para la API de OpenAI
        messages = [
            {"role": "system", "content": prompt},
            {"role": "user", "content": texto}
        ]
        response = openai.chat.completions.create(
            model=gpt_model,
            messages=messages,
            temperature=0.5
        )
        # Obtener la respuesta del modelo
        resum = response.choices[0].message.content
        return resum
    except Exception as e:
        print(f"Error al procesar el texto: {e}")
        return texto  # Devuelve el texto original en caso de error
