import fitz 
import re
import requests

def download_and_extract_pdf_text(url):
    """
    Descarga un PDF desde una URL y extrae su texto sin guardar el archivo localmente.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Verificar que la descarga sea exitosa
        with fitz.open(stream=response.content, filetype="pdf") as pdf:
            text = ""
            for page in pdf:
                text += page.get_text()
        return text
    except Exception as e:
        print(f"Error al procesar el PDF desde {url}: {e}")
        return None

def clean_pdf_text(pdf_text):
    # Eliminar espacios al inicio y final del texto
    cleaned_text = pdf_text.strip()
    # Reemplazar saltos de línea (\n) y tabulaciones (\t) por un espacio simple
    cleaned_text = re.sub(r'[\n\t]', ' ', cleaned_text)
    # Reemplazar múltiples espacios consecutivos por un solo espacio
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text)
    return cleaned_text
