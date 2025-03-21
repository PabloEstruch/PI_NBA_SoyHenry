import numpy as np
import pandas as pd
import pyodbc 
from sqlalchemy import create_engine

#lectura de los datos
df_play_by_play = pd.read_csv('Datos/play_by_play.csv', usecols=['game_id', 'eventmsgtype', 'score', 'player1_id', 'player2_id'])

#lectura de los datos de game
df_game_info = pd.read_csv("Datos/game_info.csv")

def transformar_fechas(df_game_info):
    """
    Transforma la columna 'game_date' a tipo datetime y filtra por fecha.
    """
    if 'game_date' not in df_game_info.columns:
        raise ValueError("El DataFrame df_game_info no tiene la columna 'game_date'.")
    
    df_game_info['game_date'] = pd.to_datetime(df_game_info['game_date'])
    return df_game_info[df_game_info['game_date'] > '2018-10-01']

# Función para filtrar por asistencia
def filtrar_publico(df_game_info):
    """
    Filtra los datos de asistencia.
    """
    if 'attendance' not in df_game_info.columns:
        raise ValueError("El DataFrame df_game_info no tiene la columna 'attendance'.")
    
    return df_game_info[df_game_info['attendance'] <= 21711]

# Función para filtrar por game_id
def filtrar_por_game_id(df_game_info, df_play_by_play):
    """
    Filtra el DataFrame df_play_by_play por los game_id presentes en df_game_info.
    """
    if 'game_id' not in df_game_info.columns or 'game_id' not in df_play_by_play.columns:
        raise ValueError("Los DataFrames no tienen la columna 'game_id'.")
    
    game_ids = set(df_game_info['game_id'].unique())
    return df_play_by_play[df_play_by_play['game_id'].isin(game_ids)]

# Función para limpiar play_by_play
def limpiar_play_by_play(df_play_by_play):
    """
    Filtra los eventos no deseados en play_by_play.
    """
    if 'eventmsgtype' not in df_play_by_play.columns:
        raise ValueError("El DataFrame df_play_by_play no tiene la columna 'eventmsgtype'.")
    
    eventos_analisis = [1, 2, 3, 4, 5, 6]
    return df_play_by_play[df_play_by_play['eventmsgtype'].isin(eventos_analisis)]

# Función para generar booleanos en la columna 'score'
def generar_booleanos_score(df_play_by_play):
    """
    Genera valores booleanos en la columna 'score'.
    """
    if 'score' not in df_play_by_play.columns:
        raise ValueError("El DataFrame df_play_by_play no tiene la columna 'score'.")
    
    df_play_by_play['score'] = df_play_by_play['score'].notna().astype(int)
    return df_play_by_play

# Función para filtrar player1_id
def filtrar_player_ids(df_play_by_play):
    """
    Filtra player1_id por valores menores a 2,000,000.
    """
    if 'player1_id' not in df_play_by_play.columns:
        raise ValueError("El DataFrame df_play_by_play no tiene la columna 'player1_id'.")
    
    return df_play_by_play[df_play_by_play['player1_id'] < 2000000]

def clean_data(df_game_info, df_play_by_play):
    df_game_info = transformar_fechas(df_game_info)
    df_game_info = filtrar_publico(df_game_info)
    df_play_by_play = filtrar_por_game_id(df_game_info, df_play_by_play)
    df_play_by_play = limpiar_play_by_play(df_play_by_play)
    df_play_by_play = generar_booleanos_score(df_play_by_play)
    df_play_by_play = filtrar_player_ids(df_play_by_play)
    
    return df_play_by_play

# Limpiar los datos
df_play_by_play_limpio = clean_data(df_game_info, df_play_by_play)

#guardar el archivo
df_play_by_play_limpio.to_csv('Datos/limpios/play_by_play.csv')

#Leer el CSV
#df = pd.read_csv('csv/play_by_play.csv')
df = df_play_by_play_limpio 

#Parámetros de conexión a SQL Server
server = 'server-sql-grupo1.database.windows.net'
database = 'NBA'
username = 'Admon'
password = 'Password.Server1'

#Crear la URL de conexión
connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server'

#Crear el motor de conexión
engine = create_engine(connection_string)

#Subir el DataFrame a SQL Server (especificando el nombre de la tabla)
df.to_sql('play_by_play_py.csv', con=engine, if_exists='replace', index=False)
