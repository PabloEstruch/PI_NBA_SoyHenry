import os
from google.cloud import storage
import json
import kaggle

#Configuración de Kaggle
dataset_name = "wyattowalsh/basketball"
download_path = "./Datos/descargados"
csv_folder = os.path.join(download_path, "csv")  # Ruta a la subcarpeta "csv"

#Descarga el dataset de Kaggle
kaggle.api.authenticate()
kaggle.api.dataset_download_files(dataset_name, path=download_path, unzip=True)

print(f"Dataset descargado en {download_path}")


csv_folder = "./Datos/descargados/csv"  # Ruta a la subcarpeta "csv"
credentials_path = #credenciales


#Lee el archivo de credenciales
with open(credentials_path, 'r') as f:
    credentials_json = json.load(f)

# Crea un cliente de almacenamiento con las credenciales
credentials = storage.Client.from_service_account_info(credentials_json)

# Configuración de GCS
bucket_name = "proyecto_final_nba"
gcs_client = storage.Client()
bucket = credentials.bucket(bucket_name)


# Lista de archivos a subir a GCS (con extensiones .csv)
files_to_upload = ["game.csv", "game_info.csv", "team_details.csv", "common_player.csv", "line_score.csv,"
"other_stats.csv", "draft_history.csv", "common_player_info.csv" ]


# Sube los archivos a GCS
for filename in files_to_upload:
    file_path = os.path.join(csv_folder, filename)
    if os.path.exists(file_path):
        blob = bucket.blob(filename)
        blob.upload_from_filename(file_path)
        print(f"Archivo {filename} subido a gs://{bucket_name}/{filename}")
    else:
        print(f"Archivo {filename} no encontrado en {file_path}")

print("Proceso completado.")