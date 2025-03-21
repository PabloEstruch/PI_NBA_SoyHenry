import pandas as pd

def drop_columns_from_dfs(dfs: dict, columns_to_drop: dict):
    """
    Elimina columnas específicas de los DataFrames en el diccionario dfs.
    """
    for df_name, columns in columns_to_drop.items():
        if df_name in dfs:
            dfs[df_name].drop(columns=columns, axis=1, inplace=True, errors='ignore')
    return dfs

def transformar_fechas(df_game_info):
    """
    Transforma la columna 'game_date' a tipo datetime y filtra por fecha.
    """
    df_game_info['game_date'] = pd.to_datetime(df_game_info['game_date'])
    return df_game_info[df_game_info['game_date'] > '2018-10-01']

def completar_nulos_team_details(df_team_details):
    """
    Completa los valores nulos en df_team_details con nuevos registros.
    """
    nuevos_registros = [
    {
        'team_id': '1610612738',
        'abbreviation': 'BOS',
        'nickname': 'Celtics',
        'yearfounded': '1946',
        'city': 'Boston',
        'arena': 'TD Garden',
        'arenacapacity': '18624',
        'owner': 'Wyc Grousbeck',
        'generalmanager': 'Brad Stevens',
        'headcoach': 'Joe Mazzulla',
        'facebook': 'https://web.facebook.com/bostonceltics/',
        'instagram': 'https://www.instagram.com/celtics/',
        'twitter': 'https://x.com/celtics'
    },
    {
        'team_id': '1610612739',
        'abbreviation': 'CLE',
        'nickname': 'Cavaliers',
        'yearfounded': '1970',
        'city': 'cleveland',
        'arena': 'Rocket Arena',
        'arenacapacity': '19432',
        'owner': 'Dan Gilbert',
        'generalmanager': 'Mike Gansey',
        'headcoach': 'Kenny Atkinson',
        'facebook': 'https://web.facebook.com/Cavs/',
        'instagram': 'https://www.instagram.com/cavs/',
        'twitter': 'https://x.com/cavs'
    },
    {
        'team_id': '1610612740',
        'abbreviation': 'NOP',
        'nickname': 'Pelicans',
        'yearfounded': '2002',
        'city': 'New Orleans',
        'arena': 'New Orleans Arena',
        'arenacapacity': '17791',
        'owner': 'Gayle Benson',
        'generalmanager': 'Bryson Graham',
        'headcoach': 'Willie Green',
        'facebook': 'https://web.facebook.com/PelicansNBA/',
        'instagram': 'https://www.instagram.com/pelicansnba/',
        'twitter': 'https://x.com/PelicansNBA'
    },
    {
        'team_id': '1610612752',
        'abbreviation': 'NYK',
        'nickname': 'Knicks',
        'yearfounded': '1946',
        'city': 'New York',
        'arena': 'Madison Square Garden',
        'arenacapacity': '19500',
        'owner': 'James L. Dolan',
        'generalmanager': 'Gersson Rosas',
        'headcoach': 'Tom Thibodeau',
        'facebook': 'https://web.facebook.com/NYKnicks',
        'instagram': 'https://www.instagram.com/nyknicks',
        'twitter': 'https://x.com/nyknicks'
    },
    {
        'team_id': '1610612753',
        'abbreviation': 'ORL',
        'nickname': 'Magic',
        'yearfounded': '1989',
        'city': 'Orlando',
        'arena': 'Kia Center',
        'arenacapacity': '20000',
        'owner': 'RDV Sports Inc.',
        'generalmanager': 'Anthony Parker',
        'headcoach': 'Jamahl Mosley',
        'facebook': 'https://web.facebook.com/OrlandoMagic/',
        'instagram': 'https://www.instagram.com/orlandomagic/',
        'twitter': 'https://x.com/OrlandoMagic'
    }
    ]
    return pd.concat([df_team_details, pd.DataFrame(nuevos_registros)], ignore_index=True)

def actualizar_arenas_y_entrenadores(df_team_details):
    """
    Actualiza las capacidades de las arenas y los entrenadores.
    """
    arena_capacities = {
    "Nuggets": 21000, "Warriors": 18064, "Nets": 19000, "76ers": 21000,
    "Suns": 18422, "Thunder": 18203, "Raptors": 19800, "Jazz": 20000, "Pistons": 20491
    }
    mascara_arenacapacity_vacio = df_team_details["arenacapacity"].isna() | (df_team_details["arenacapacity"] == "")
    df_team_details.loc[mascara_arenacapacity_vacio, "arenacapacity"] = df_team_details["nickname"].map(arena_capacities)

    # Actualizar solo los valores en blanco en 'headcoach' para los Raptors
    mascara_headcoach_vacio = (df_team_details["nickname"] == "Raptors") & (df_team_details["headcoach"].isna() | (df_team_details["headcoach"] == ""))
    df_team_details.loc[mascara_headcoach_vacio, "headcoach"] = "Darko Rajakovic"

    return df_team_details
    
def agregar_conferencias_y_nombres(df_team_details):
    """
    Agrega columnas de conferencia y nombre completo.
    """
    equipos_este = {"BOS", "BKN", "NYK", "PHI", "TOR", "ATL", "CHA", "MIA", "ORL", "WAS", "CHI", "CLE", "DET", "IND", "MIL"}
    df_team_details["Conferencia"] = df_team_details["abbreviation"].map(lambda x: "East" if x in equipos_este else "West")
    df_team_details["full_name"] = df_team_details[["city", "nickname"]].agg(" ".join, axis=1)
    return df_team_details

def filtrar_tipos_de_juego(df_game):
    """
    Filtra los tipos de juego no deseados.
    """
    return df_game[~df_game['season_type'].isin(['All-Star', 'All Star', 'Pre Season'])]

def filtrar_publico(df_game_info):
    """
    Filtra los datos de asistencia.
    """
    return df_game_info[df_game_info['attendance'] <= 21711]

def llenar_nulos_con_cero(dfs, columnas_por_df):
    """
    Llena valores nulos en las columnas especificadas.
    """
    for df_name, columnas in columnas_por_df.items():
        if df_name in dfs:
            dfs[df_name][columnas] = dfs[df_name][columnas].fillna("")
    return dfs

def filtrar_por_game_id(df_game_info, *dfs):
    """
    Filtra los DataFrames por game_id.
    """
    game_ids = set(df_game_info['game_id'].unique())
    return [df[df['game_id'].isin(game_ids)] for df in dfs]

def limpiar_play_by_play(df_play_by_play):
    """
    Filtra los eventos no deseados en play_by_play.
    """
    eventos_analisis = [1, 2, 3, 4, 5, 6]
    return df_play_by_play[df_play_by_play['eventmsgtype'].isin(eventos_analisis)]

def generar_booleanos_score(df_play_by_play):
    """
    Genera valores booleanos en la columna 'score'.
    """
    df_play_by_play['score'] = df_play_by_play['score'].notna().astype(int)
    return df_play_by_play

def filtrar_player_ids(df_play_by_play):
    """
    Filtra player1_id por valores menores a 2,000,000.
    """
    return df_play_by_play[df_play_by_play['player1_id'] < 2000000]