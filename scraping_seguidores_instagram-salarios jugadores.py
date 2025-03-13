#!pip install selenium
#!pip install webdriver-manager ----------- esto necesita un chromedriver.exe en el ambiente
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
#import pandas as pd ----------------si no lo tiene

df = pd.read_csv('teams_final.csv') #--------------------- aqui va team_details. no se como quedo

# Configurar el navegador (modo headless para que no abra ventana)
options = webdriver.ChromeOptions()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

# Iniciar el navegador
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

def get_followers(instagram_url):
    """Obtiene el número de seguidores de un perfil de Instagram"""
    if not isinstance(instagram_url, str) or "instagram.com" not in instagram_url:
        return None  # Si el valor no es una URL válida, devolver None

    try:
        driver.get(instagram_url)
        time.sleep(5)  # Esperar a que cargue la página

        # Buscar el número de seguidores en la página
        followers_element = driver.find_element(By.XPATH, "//span[contains(@title, ',') or contains(text(), 'M') or contains(text(), 'K')]")
        followers = followers_element.text
        return followers
    except Exception as e:
        print(f"Error con {instagram_url}: {e}")
        return None  # Si falla, devolver None

# Aplicar la función a la columna 'instagram' y crear la nueva columna
df["seguidores_instagram"] = df["instagram"].apply(get_followers)

df.to_csv('')#------------------aqui va el nombre final



#Para sacar salarios de jugadores

# URL de la página
url = "https://hoopshype.com/salaries/players/2022-2023/"
driver.get(url)

# Esperar unos segundos para que cargue la página
time.sleep(5)

# Extraer nombres de los jugadores
players = driver.find_elements(By.XPATH, "//td[@class='name']/a")
players_list = [player.text.strip() for player in players]  # Extraemos el texto

# Extraer los salarios
salaries = driver.find_elements(By.XPATH, "//td[@class='hh-salaries-sorted']")
salaries_list = [salary.text.strip() for salary in salaries]  # Extraemos el texto
del salaries_list[0]


# Cerrar Selenium
driver.quit()

# Crear DataFrame
df = pd.DataFrame({"Jugador": players_list, "Salario": salaries_list})

# Guardar en CSV
df.to_csv("salarios_nba_2022_2023.csv", index=False)