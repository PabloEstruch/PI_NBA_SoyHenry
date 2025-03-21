from flask import jsonify
import os
import pandas as pd
import pymssql
import pyodbc
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from utils.data_cleaning import  actualizar_arenas_y_entrenadores, agregar_conferencias_y_nombres, completar_nulos_team_details, drop_columns_from_dfs, filtrar_por_game_id, filtrar_publico, filtrar_tipos_de_juego, llenar_nulos_con_cero, transformar_fechas
from io import StringIO
from google.cloud import storage


##EXTRACCION
# Configura el cliente de Google Cloud Storage
client = storage.Client()

# Nombre del bucket
bucket_name = 'proyecto_final_nba'
bucket = client.bucket(bucket_name)

# Función para cargar un archivo CSV desde GCS en un DataFrame
def load_csv_from_gcs(file_name):
    blob = bucket.blob(file_name)
    data = blob.download_as_text() 
    return pd.read_csv(StringIO(data))  

# Cargar los archivos en un diccionario de DataFrames
dfs = {
    "df_game": load_csv_from_gcs("game.csv"),
    "df_other_stats": load_csv_from_gcs("other_stats.csv"),
    "df_line": load_csv_from_gcs("line_score.csv"),
    "df_common_player": load_csv_from_gcs("common_player_info.csv"),
    "df_draft_history": load_csv_from_gcs("draft_history.csv"),
    "df_game_info": load_csv_from_gcs("game_info.csv"),
    "df_team_details": load_csv_from_gcs("team_details.csv")  
}

# Verifica que los DataFrames se cargaron correctamente
for key, df in dfs.items():
    print(f"{key}: {df.shape}")

#LIMPIEZA
def clean_data(dfs):
         columns_to_drop = {
              'df_game_info': ['game_time'],
              'df_game': ['team_abbreviation_home',	'team_name_home', 'video_available_away', 'matchup_home', 'matchup_away', 'team_abbreviation_away', 'team_name_away', 'video_available_home', 'game_date'],
              'df_team_details': [ 'dleagueaffiliation'], 
              'df_other_stats': ['league_id','team_abbreviation_home', 'team_city_home','largest_lead_home', 'lead_changes', 'times_tied', 'team_turnovers_home','team_rebounds_home','team_abbreviation_away', 'team_city_away', 'largest_lead_away', 'team_turnovers_away','team_rebounds_away'],
              'df_line': ["pts_ot5_home","pts_ot6_home","pts_ot7_home","pts_ot8_home","pts_ot9_home","pts_ot10_home","pts_ot5_away","pts_ot6_away", "pts_ot7_away","pts_ot8_away","pts_ot9_away","pts_ot10_away","game_sequence","team_abbreviation_home","team_city_name_home", "team_nickname_home","team_abbreviation_away","team_city_name_away","team_nickname_away", "team_wins_losses_home","team_wins_losses_away"],
              'df_common_player': ['display_first_last', 'display_last_comma_first', 'display_fi_last', 'player_slug', 'last_affiliation', 'team_name', 'team_code', 'dleague_flag', 'nba_flag', 'games_played_flag', 'greatest_75_flag', 'games_played_current_season_flag','school', 'team_abbreviation', 'team_city'],
              'df_draft_history': ['player_profile_flag', 'draft_type', 'player_name',	'round_number',	'round_pick', 'overall_pick','team_city', 'team_name', 'team_abbreviation', 'organization', 'organization_type']
              }
         columnas_por_df = {
              "df_other_stats": ['total_turnovers_home', 'pts_off_to_home', 'total_turnovers_away', 'pts_off_to_away'],
              "df_game": ['ft_pct_home']
              }
         dfs = drop_columns_from_dfs(dfs, columns_to_drop)
         dfs['df_game_info'] = transformar_fechas(dfs['df_game_info'])
         dfs['df_game_info'] = filtrar_publico(dfs['df_game_info'])
         dfs['df_team_details'] = completar_nulos_team_details(dfs['df_team_details'])
         dfs['df_team_details'] = actualizar_arenas_y_entrenadores(dfs['df_team_details'])
         dfs['df_team_details'] = agregar_conferencias_y_nombres(dfs['df_team_details'])
         dfs['df_game'] = filtrar_tipos_de_juego(dfs['df_game'])
         dfs = llenar_nulos_con_cero(dfs, columnas_por_df)
         dfs['df_game'], dfs['df_other_stats'], dfs['df_line'] = filtrar_por_game_id(dfs['df_game_info'], dfs['df_game'], dfs['df_other_stats'], dfs['df_line'])
         return dfs

dfs_limpios = clean_data(dfs)



#CARGA a AZURE
# Configurar conexión a SQL Server
server = 'server-sql-grupo1.database.windows.net'
database = 'NBA'
username = 'Admon'
password = 'Password.Server1'

# Crear el engine usando pymssql
connection_string = f"mssql+pymssql://{username}:{password}@{server}/{database}"
engine = create_engine(connection_string)

Session = sessionmaker(bind=engine)
session = Session()

try:
    for table_name, df in dfs.items():
        try:
            # Cargar el DataFrame en la base de datos
            df.to_sql(table_name, con=engine, if_exists='replace', index=False)
            print(f"Tabla '{table_name}' cargada correctamente.")
        
        except Exception as e:
            session.rollback()  # Hacer rollback en caso de error
            print(f"Error al cargar la tabla '{table_name}': {e}")
            continue  # Continuar con el siguiente DataFrame

    # Si todo sale bien, hacer commit de la transacción
    session.commit()

except Exception as e:
    # En caso de error general, hacer rollback de todo
    session.rollback()
    print(f"Error en la transacción: {e}")

finally:
    # Asegurarse de cerrar la sesión al final
    session.close()

