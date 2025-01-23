from bs4 import BeautifulSoup
import requests

# Asegurar que todas las URLs tengan esquema (http/https)
def asegurar_esquema(url):
    """
    Asegura que las URLs tengan un esquema válido (http/https).
    """
    if not url or url.strip() == '' or url == 'nan':
        return None
    if not url.startswith(('http://', 'https://')):
        return f'https://{url}'
    return url

# Configuración de encabezados para solicitudes HTTP
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/58.0.3029.110 Safari/537.3"
    )
}

# Extraer contenido de una subpágina
def extract_subpage_content(link):
    """
    Extrae el contenido de una subpágina dada una URL.
    """
    try:
        sub_response = requests.get(link, headers=HEADERS)
        sub_response.raise_for_status()
        sub_soup = BeautifulSoup(sub_response.content, 'lxml')

        # Extraer título y párrafos de la subpágina
        titulo = sub_soup.title.string if sub_soup.title else "Sin título"
        parrafos = [p.get_text(strip=True) for p in sub_soup.find_all('p')]
        contenido = "\n".join(parrafos)

        return {"titulo": titulo, "contenido": contenido}
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}

# Extraer contenido de una URL principal y sus subpáginas
def extract_content_from_url(url):
    """
    Extrae contenido de una URL principal y sus subpáginas (si las hay).
    """
    if not url:
        return "URL vacía o inválida."

    try:
        # Realizar solicitud HTTP a la URL principal
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')

        # Extraer título y contenido de la página principal
        main_parrafos = [p.get_text(strip=True) for p in soup.find_all('p')]
        main_content = "\n".join(main_parrafos)

        # Extraer enlaces de navegación (si existen)
        nav_section = soup.find('nav')
        if not nav_section:
            return main_content  # Si no hay navegación, devolver solo el contenido principal

        # Procesar enlaces en la sección de navegación
        links = [
            link if link.startswith('http') else requests.compat.urljoin(url, link)
            for link in {a['href'] for a in nav_section.find_all('a', href=True) if a['href'] != '#'}
        ]

        # Extraer contenido de cada subpágina
        contenidos_enlaces = {
            link: extract_subpage_content(link)
            for link in links
        }

        # Combinar contenido de la página principal y subpáginas
        contenido_subpaginas = "\n\n".join(
            [f"Título: {datos.get('titulo', 'No disponible')}\nContenido:\n{datos.get('contenido', 'No se pudo extraer el contenido')}"
             for link, datos in contenidos_enlaces.items()]
        )
        contenido_final = f"{main_content}\n\nContenido de las subpáginas:\n{contenido_subpaginas}"

        return contenido_final

    except requests.exceptions.RequestException as e:
        return f"Error al acceder a {url}: {e}"
