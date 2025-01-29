import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
import re  # Importar para usar clean_text
from PyPDF2 import PdfReader
import tiktoken

# Configuración de encabezados para solicitudes HTTP
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/58.0.3029.110 Safari/537.3"
    )
}

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
def extract_visible_links(url, base_url):
    try:
        response = requests.get(url, timeout=10, headers=HEADERS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        
        links = set()
        for a_tag in soup.find_all("a", href=True):
            # Verificar que el enlace sea visible
            if a_tag.get_text(strip=True):  # Solo enlaces con texto visible
                link = urljoin(base_url, a_tag["href"])  # Convertir a enlaces absolutos
                # Solo incluir enlaces dentro del dominio principal
                if urlparse(link).netloc == urlparse(base_url).netloc:
                    links.add(link)
        
        return links
    except requests.RequestException as e:
        print(f"Error al extraer enlaces de {url}: {e}")
        return set()

# Función para extraer texto visible de una página
def extract_text(url):
    try:
        response = requests.get(url, timeout=10, headers=HEADERS)
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
def download_and_extract_pdf(url, path):
    try:
        response = requests.get(url, stream=True, timeout=10, headers=HEADERS)
        response.raise_for_status()

        pdf_filename = path + f"pdfs/{hash(url)}.pdf"
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


def crawl_and_accumulate_with_token_limit(base_url, path, gpt_model="gpt-4o-mini", max_pages=1000, max_tokens=128000):
    visited = set()
    to_visit = {base_url}
    page_count = 0
    accumulated_text = ""  # Variable para acumular texto

    # Inicializar el tokenizador para el modelo GPT
    tokenizer = tiktoken.encoding_for_model(gpt_model)

    while to_visit and len(visited) < max_pages:
        current_url = to_visit.pop()
        if current_url in visited:
            continue

        print(f"Visitando: {current_url}")
        visited.add(current_url)
        page_count += 1

        try:
            if current_url.lower().endswith(".pdf"):
                # Procesar PDFs y acumular texto
                pdf_text = download_and_extract_pdf(current_url, path)
                cleaned_pdf_text = clean_text(pdf_text)
                accumulated_text += f"\n\nURL: {current_url}\n{cleaned_pdf_text}"
            else:
                # Procesar texto de la página HTML y acumular
                page_text = extract_text(current_url)
                cleaned_page_text = clean_text(page_text)
                accumulated_text += f"\n\nURL: {current_url}\n{cleaned_page_text}"

                # Extraer enlaces adicionales y añadirlos a `to_visit`
                links = extract_visible_links(current_url, base_url)
                to_visit.update(links - visited)

            # Calcular el número de tokens acumulados
            tokens = tokenizer.encode(accumulated_text)
            num_tokens = len(tokens)

            print(f"Tokens acumulados: {num_tokens}/{max_tokens}")

            # Verificar si se ha alcanzado el límite de tokens
            if num_tokens >= max_tokens:
                print("Límite de tokens alcanzado. Deteniendo el proceso.")
                break

        except Exception as e:
            print(f"Error procesando {current_url}: {e}")

        time.sleep(1)

    print("Proceso de web scraping completado.")
    return accumulated_text
