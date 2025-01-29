import requests
from bs4 import BeautifulSoup
import re

def clean_text(text):
    # Eliminar espacios al inicio y final del texto
    cleaned_text = text.strip()
    # Reemplazar saltos de línea (\n) y tabulaciones (\t) por un espacio simple
    cleaned_text = re.sub(r'[\n\t]', ' ', cleaned_text)
    # Reemplazar múltiples espacios consecutivos por un solo espacio
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text)
    return cleaned_text

def extract_text(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        
        text = soup.get_text(separator="\n").strip()
        return text
    except requests.RequestException as e:
        print(f"Error al extraer texto de {url}: {e}")
        return ""

# page_text = extract_text("https://www.mineducacion.gov.co/portal/micrositios-superior/Becas-SER/")
# cleaned_page_text = clean_text(page_text)
# print(cleaned_page_text)