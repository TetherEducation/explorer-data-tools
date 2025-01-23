import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import os
import time
import re  # Importar para usar clean_text
from PyPDF2 import PdfReader



# Función para limpiar texto
def clean_text(text):
    # Eliminar espacios al inicio y final del texto
    cleaned_text = text.strip()
    # Reemplazar saltos de línea (\n) y tabulaciones (\t) por un espacio simple
    cleaned_text = re.sub(r'[\n\t]', ' ', cleaned_text)
    # Reemplazar múltiples espacios consecutivos por un solo espacio
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text)
    return cleaned_text

# Función para extraer enlaces de una página
def extract_links(url, base_url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        
        links = set()
        for a_tag in soup.find_all("a", href=True):
            link = urljoin(base_url, a_tag["href"])  # Convertir enlaces relativos a absolutos
            if urlparse(link).netloc == urlparse(base_url).netloc:  # Solo enlaces del mismo dominio
                links.add(link)
        return links
    except requests.RequestException as e:
        print(f"Error al extraer enlaces de {url}: {e}")
        return set()

# Función para extraer texto visible de una página
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

# Función para descargar y leer un PDF
def download_and_extract_pdf(url, output_path):
    try:
        response = requests.get(url, stream=True, timeout=10)
        response.raise_for_status()

        pdf_filename = f"{output_path}/pdfs/{hash(url)}.pdf"
        with open(pdf_filename, "wb") as pdf_file:
            for chunk in response.iter_content(chunk_size=1024):
                pdf_file.write(chunk)
        print(f"PDF descargado: {pdf_filename}")

        pdf_text = ""
        with open(pdf_filename, "rb") as file:
            reader = PdfReader(file)
            for page in reader.pages:
                pdf_text += page.extract_text() + "\n"
        
        return pdf_text

    except Exception as e:
        print(f"Error al procesar el PDF {url}: {e}")
        return ""

# Función principal para rastrear y extraer texto de todo el sitio
def crawl_and_extract(base_url, output_path, max_pages=50):
    visited = set()
    to_visit = {base_url}
    page_count = 0

    while to_visit and len(visited) < max_pages:
        current_url = to_visit.pop()
        if current_url in visited:
            continue

        print(f"Visitando: {current_url}")
        visited.add(current_url)
        page_count += 1
        
        try:
            if current_url.lower().endswith(".pdf"):
                pdf_text = download_and_extract_pdf(current_url, output_path)
                cleaned_pdf_text = clean_text(pdf_text)
                save_text_to_file(f"{output_path}/html_texts/link_{page_count}.txt", cleaned_pdf_text, current_url)
            else:
                page_text = extract_text(current_url)
                cleaned_page_text = clean_text(page_text)
                save_text_to_file(f"{output_path}/html_texts/link_{page_count}.txt", cleaned_page_text, current_url)

                # Extraer enlaces adicionales
                links = extract_links(current_url, base_url)
                to_visit.update(links - visited)

        except Exception as e:
            print(f"Error procesando {current_url}: {e}")

        time.sleep(1)

    print("Proceso completado.")

# Función para guardar texto en un archivo
def save_text_to_file(filename, text, url):
    try:
        with open(filename, "w", encoding="utf-8") as file:
            file.write(f"URL: {url}\n\n")
            file.write(text)
        print(f"Texto guardado en {filename}")
    except Exception as e:
        print(f"Error guardando el texto de {url}: {e}")