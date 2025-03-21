{"nbformat":4,"nbformat_minor":0,"metadata":{"colab":{"provenance":[{"file_id":"1T7gFGTHTm06Nb4WzWHf0HT9cC0wQ3EXg","timestamp":1741875231811}],"gpuType":"V28"},"kernelspec":{"name":"python3","display_name":"Python 3"},"language_info":{"name":"python"},"accelerator":"TPU"},"cells":[{"cell_type":"code","source":["pip install pyodbc"],"metadata":{"colab":{"base_uri":"https://localhost:8080/"},"id":"jRzFNSWHHxBq","executionInfo":{"status":"ok","timestamp":1741888014779,"user_tz":180,"elapsed":4316,"user":{"displayName":"Francisco Diaz Molina","userId":"06591181027171350894"}},"outputId":"3501dd79-1484-4eba-93c3-c3979fea68eb"},"execution_count":3,"outputs":[{"output_type":"stream","name":"stdout","text":["Collecting pyodbc\n","  Downloading pyodbc-5.2.0-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl.metadata (2.7 kB)\n","Downloading pyodbc-5.2.0-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl (346 kB)\n","\u001b[?25l   \u001b[90m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\u001b[0m \u001b[32m0.0/346.2 kB\u001b[0m \u001b[31m?\u001b[0m eta \u001b[36m-:--:--\u001b[0m\r\u001b[2K   \u001b[91m━━━━━━━━━━━━━━━━\u001b[0m\u001b[91m╸\u001b[0m\u001b[90m━━━━━━━━━━━━━━━━━━━━━━━\u001b[0m \u001b[32m143.4/346.2 kB\u001b[0m \u001b[31m4.1 MB/s\u001b[0m eta \u001b[36m0:00:01\u001b[0m\r\u001b[2K   \u001b[91m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\u001b[0m\u001b[90m╺\u001b[0m \u001b[32m337.9/346.2 kB\u001b[0m \u001b[31m6.5 MB/s\u001b[0m eta \u001b[36m0:00:01\u001b[0m\r\u001b[2K   \u001b[90m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\u001b[0m \u001b[32m346.2/346.2 kB\u001b[0m \u001b[31m4.6 MB/s\u001b[0m eta \u001b[36m0:00:00\u001b[0m\n","\u001b[?25hInstalling collected packages: pyodbc\n","Successfully installed pyodbc-5.2.0\n"]}]},{"cell_type":"code","source":["pip install pymssql"],"metadata":{"colab":{"base_uri":"https://localhost:8080/"},"id":"XoJ1DrfZnc9_","executionInfo":{"status":"ok","timestamp":1741888033518,"user_tz":180,"elapsed":1962,"user":{"displayName":"Francisco Diaz Molina","userId":"06591181027171350894"}},"outputId":"79b97ef2-3a84-428e-c6d4-8fd4f92649a5"},"execution_count":5,"outputs":[{"output_type":"stream","name":"stdout","text":["Collecting pymssql\n","  Downloading pymssql-2.3.2-cp311-cp311-manylinux_2_28_x86_64.whl.metadata (4.7 kB)\n","Downloading pymssql-2.3.2-cp311-cp311-manylinux_2_28_x86_64.whl (4.8 MB)\n","\u001b[2K   \u001b[90m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\u001b[0m \u001b[32m4.8/4.8 MB\u001b[0m \u001b[31m38.0 MB/s\u001b[0m eta \u001b[36m0:00:00\u001b[0m\n","\u001b[?25hInstalling collected packages: pymssql\n","Successfully installed pymssql-2.3.2\n"]}]},{"cell_type":"code","source":["pip install pyodbc"],"metadata":{"colab":{"base_uri":"https://localhost:8080/"},"id":"gurOTtG1nhxV","executionInfo":{"status":"ok","timestamp":1741888044226,"user_tz":180,"elapsed":1443,"user":{"displayName":"Francisco Diaz Molina","userId":"06591181027171350894"}},"outputId":"b51f9cf8-094b-4f35-f2ed-7720068c80d2"},"execution_count":6,"outputs":[{"output_type":"stream","name":"stdout","text":["Requirement already satisfied: pyodbc in /usr/local/lib/python3.11/dist-packages (5.2.0)\n"]}]},{"cell_type":"code","source":["pip install sqlalchemy"],"metadata":{"colab":{"base_uri":"https://localhost:8080/"},"id":"ONFcglGTnqLo","executionInfo":{"status":"ok","timestamp":1741888079513,"user_tz":180,"elapsed":3774,"user":{"displayName":"Francisco Diaz Molina","userId":"06591181027171350894"}},"outputId":"9ee2c16f-83bd-43da-8e0d-9c0ae3ebb873"},"execution_count":8,"outputs":[{"output_type":"stream","name":"stdout","text":["Collecting sqlalchemy\n","  Downloading sqlalchemy-2.0.39-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl.metadata (9.6 kB)\n","Collecting greenlet!=0.4.17 (from sqlalchemy)\n","  Downloading greenlet-3.1.1-cp311-cp311-manylinux_2_24_x86_64.manylinux_2_28_x86_64.whl.metadata (3.8 kB)\n","Requirement already satisfied: typing-extensions>=4.6.0 in /usr/local/lib/python3.11/dist-packages (from sqlalchemy) (4.12.2)\n","Downloading sqlalchemy-2.0.39-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl (3.2 MB)\n","\u001b[2K   \u001b[90m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\u001b[0m \u001b[32m3.2/3.2 MB\u001b[0m \u001b[31m29.1 MB/s\u001b[0m eta \u001b[36m0:00:00\u001b[0m\n","\u001b[?25hDownloading greenlet-3.1.1-cp311-cp311-manylinux_2_24_x86_64.manylinux_2_28_x86_64.whl (602 kB)\n","\u001b[2K   \u001b[90m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\u001b[0m \u001b[32m602.4/602.4 kB\u001b[0m \u001b[31m22.3 MB/s\u001b[0m eta \u001b[36m0:00:00\u001b[0m\n","\u001b[?25hInstalling collected packages: greenlet, sqlalchemy\n","Successfully installed greenlet-3.1.1 sqlalchemy-2.0.39\n"]}]},{"cell_type":"code","source":["from google.colab import drive\n","drive.mount('/content/drive')\n"],"metadata":{"colab":{"base_uri":"https://localhost:8080/"},"id":"s-dq2_7KoWj2","executionInfo":{"status":"ok","timestamp":1741888318436,"user_tz":180,"elapsed":64469,"user":{"displayName":"Francisco Diaz Molina","userId":"06591181027171350894"}},"outputId":"32862017-bad4-4026-e574-8bae06292fff"},"execution_count":12,"outputs":[{"output_type":"stream","name":"stdout","text":["Mounted at /content/drive\n"]}]},{"cell_type":"code","source":["#importar librerias\n","import pandas as pd\n","import pymssql\n","import pyodbc\n","from sqlalchemy import create_engine\n","import numpy as np\n","import zipfile\n","import os\n","#READ\n","\n","# 🔹 Montar Google Drive\n","from google.colab import files\n","drive.mount('/content/drive')\n","kaggle_json_path = \"/content/drive/My Drive/Proyecto Final/kaggle.json\"\n","\n","\n","\n","# Crear carpeta .kaggle en el directorio raíz del usuario\n","os.makedirs(\"/root/.kaggle\", exist_ok=True)\n","\n","# Mover kaggle.json a la carpeta\n","!mv kaggle.json /root/.kaggle/\n","\n","# Cambiar permisos para evitar problemas de acceso\n","!chmod 600 /root/.kaggle/kaggle.json\n","\n","!kaggle datasets download -d wyattowalsh/basketball\n","\n","\n","# Extraer los archivos en una carpeta llamada \"basketball_data\"\n","with zipfile.ZipFile(\"basketball.zip\", \"r\") as zip_ref:\n","    zip_ref.extractall(\"basketball_data\")\n","\n","# Listar archivos en la carpeta\n","os.listdir(\"basketball_data\")\n","\n","#lecura de archivos a usar (conectar a la API, Seleccionar de un Bucket)\n","#df_team eliminar\n","df_game=pd.read_csv(\"basketball_data/csv/game.csv\")\n","df_team_details=pd.read_csv('basketball_data/csv/team_details.csv')\n","df_other_stats=pd.read_csv('basketball_data/csv/other_stats.csv')\n","df_line=pd.read_csv('basketball_data/csv/line_score.csv')\n","df_common_player=pd.read_csv('basketball_data/csv/common_player_info.csv')\n","df_draft_history=pd.read_csv('basketball_data/csv/draft_history.csv')\n","df_game_info=pd.read_csv('basketball_data/csv/game_info.csv')\n","df_play_by_play=pd.read_csv('basketball_data/csv/play_by_play.csv')\n","\n","#limpieza\n","\n","\n","dfs = {\n","    'df_game': df_game,\n","    #'df_team': df_team,  #eliminar\n","    'df_team_details': df_team_details,\n","    'df_other_stats': df_other_stats,\n","    'df_line': df_line,\n","    'df_common_player': df_common_player,\n","    'df_draft_history': df_draft_history,\n","    'df_game_info': df_game_info,\n","    'df_play_by_play': df_play_by_play\n","}\n","\n","#eliminar columnas de dataframes\n","columns_to_drop = {\n","    'df_game': ['team_abbreviation_home',\t'team_name_home', 'video_available_away', 'matchup_home',\n","                'matchup_away', 'team_abbreviation_away', 'team_name_away', 'video_available_home'], #, 'game_date'\n","    'df_team_details': [ 'dleagueaffiliation'], #'id'\n","    'df_other_stats': ['league_id', 'team_abbreviation_home', 'team_city_home', 'team_abbreviation_away', 'team_city_away',\n","                       'largest_lead_home', 'largest_lead_away', 'lead_changes', 'times_tied', 'team_turnovers_home', 'team_turnovers_away',\n","                       'team_rebounds_home', 'team_rebounds_away'],\n","    'df_line': [\"pts_ot5_home\",\"pts_ot6_home\",\"pts_ot7_home\",\"pts_ot8_home\",\"pts_ot9_home\",\"pts_ot10_home\",\"pts_ot5_away\",\"pts_ot6_away\",\n","                      \"pts_ot7_away\",\"pts_ot8_away\",\"pts_ot9_away\",\"pts_ot10_away\",\"game_sequence\",\"team_abbreviation_home\",\"team_city_name_home\",\n","                      \"team_nickname_home\",\"team_abbreviation_away\",\"team_city_name_away\",\"team_nickname_away\",\n","                      \"team_wins_losses_home\",\"team_wins_losses_away\"],#\"game_date_est\"\n","    'df_common_player': ['display_first_last', 'display_last_comma_first', 'display_fi_last', 'player_slug', 'last_affiliation', 'team_name',\n","                         'team_code', 'dleague_flag', 'nba_flag', 'games_played_flag', 'greatest_75_flag',\n","                         'games_played_current_season_flag','school', 'team_abbreviation', 'team_city'],#'player_code'\n","    'df_draft_history': ['player_profile_flag', 'draft_type', 'player_name',\t'round_number',\t'round_pick', 'overall_pick',\n","                         'team_city', 'team_name', 'team_abbreviation', 'organization', 'organization_type'],#, 'season'\n","    'df_game_info': ['game_time'],\n","    #'df_play_by_play': ['wctimestring', 'eventnum', 'neutraldescription', 'person1type', 'person2type', 'person3type',\n","    #                    'player1_team_city', 'player1_team_nickname', 'player2_team_city', 'player2_team_nickname', 'player3_team_city',\n","    #                    'player3_team_nickname', 'video_available_flag', 'player3_id', 'player3_name', 'player3_team_id',\n","    #                   'player3_team_abbreviation', 'eventnum', 'eventmsgactiontype', 'pctimestring', 'scoremargin', 'player1_name',\n","    #                   'player1_team_id', 'player2_name','player2_team_id']\n","}\n","\n","for df_name, columns in columns_to_drop.items():\n","    df = globals().get(df_name)\n","    if df is not None:\n","        df.drop(columns=columns, axis=1, inplace=True)\n","\n","\n","# cambio de datos game y game info ******\n","df_line = df_line.rename(columns={'game_date_est': 'game_date'})\n","\n","#Pasar date a fecha\n","\n","df_game_info['game_date'] = pd.to_datetime(df_game_info['game_date'])\n","df_game['game_date'] = pd.to_datetime(df_game['game_date'])\n","df_line['game_date'] = pd.to_datetime(df_line['game_date'])\n","\n","\n","#2018 en adelante ******\n","df_game = df_game[df_game['game_date'] >  '2018-10-01']\n","df_game_info = df_game_info[df_game_info['game_date'] >  '2018-10-01']\n","df_line = df_line[(df_line[\"game_date\"] > \"2018-10-16\")]\n","df_common_player=df_common_player[(df_common_player['from_year'] >= 2001)]\n","df_draft_history = df_draft_history[(df_draft_history[\"season\"] >= 2001)]\n","\n","#conversion de datos nulos\n","\n","#for name, df in dfs.items():\n"," #   print(f\"DataFrame: {name}\")\n","   # print(df.isnull().sum())  # Cantidad de nulos por columna\n","  #  print(f\"Total de nulos: {df.isnull().sum().sum()}\\n\")  # Total de nulos en el DataFrame\n","\n","\n","#Columnas  rellenar\n","df_game.loc[:, 'ft_pct_home'] = df_game['ft_pct_home'].fillna(0)\n","\n","#ELIMINE ESTE FOR\n","\n"," #    columnas_a_llenar = {\n"," #       df_game: 'ft_pct_home'\n"," #\n"," #   Iterar sobre cada DataFrame y sus columnas\n"," #    for df, columnas in columnas_a_llenar.items():\n"," #       df[columnas] = df[columnas].fillna(0)\n","\n","\n","\n","#completar de forma especifica los nulos team_details\n","nuevos_registros = [\n","    {\n","        'team_id': '1610612738',\n","        'abbreviation': 'BOS',\n","        'nickname': 'Celtics',\n","        'yearfounded': '1946',\n","        'city': 'Boston',\n","        'arena': 'TD Garden',\n","        'arenacapacity': '18624',\n","        'owner': 'Wyc Grousbeck',\n","        'generalmanager': 'Brad Stevens',\n","        'headcoach': 'Joe Mazzulla',\n","        'dleagueaffiliation': 'Maine Celtics',\n","        'facebook': 'https://web.facebook.com/bostonceltics/',\n","        'instagram': 'https://www.instagram.com/celtics/',\n","        'twitter': 'https://x.com/celtics'\n","    },\n","    {\n","        'team_id': '1610612739',\n","        'abbreviation': 'CLE',\n","        'nickname': 'Cavaliers',\n","        'yearfounded': '1970',\n","        'city': 'cleveland',\n","        'arena': 'Rocket Arena',\n","        'arenacapacity': '19432',\n","        'owner': 'Dan Gilbert',\n","        'generalmanager': 'Mike Gansey',\n","        'headcoach': 'Kenny Atkinson',\n","        'dleagueaffiliation': 'The Cleveland Charge',\n","        'facebook': 'https://web.facebook.com/Cavs/',\n","        'instagram': 'https://www.instagram.com/cavs/',\n","        'twitter': 'https://x.com/cavs'\n","    },\n","    {\n","        'team_id': '1610612740',\n","        'abbreviation': 'NOP',\n","        'nickname': 'Pelicans',\n","        'yearfounded': '2002',\n","        'city': 'New Orleans',\n","        'arena': 'New Orleans Arena',\n","        'arenacapacity': '17791',\n","        'owner': 'Gayle Benson',\n","        'generalmanager': 'Bryson Graham',\n","        'headcoach': 'Willie Green',\n","        'dleagueaffiliation': 'Birmingham Squadron',\n","        'facebook': 'https://web.facebook.com/PelicansNBA/',\n","        'instagram': 'https://www.instagram.com/pelicansnba/',\n","        'twitter': 'https://x.com/PelicansNBA'\n","    },\n","    {\n","        'team_id': '1610612752',\n","        'abbreviation': 'NYK',\n","        'nickname': 'Knicks',\n","        'yearfounded': '1946',\n","        'city': 'New York',\n","        'arena': 'Madison Square Garden',\n","        'arenacapacity': '19500',\n","        'owner': 'James L. Dolan',\n","        'generalmanager': 'Gersson Rosas',\n","        'headcoach': 'Tom Thibodeau',\n","        'dleagueaffiliation': 'Westchester Knicks',\n","        'facebook': 'https://web.facebook.com/NYKnicks',\n","        'instagram': 'https://www.instagram.com/nyknicks',\n","        'twitter': 'https://x.com/nyknicks'\n","    },\n","    {\n","        'team_id': '1610612753',\n","        'abbreviation': 'ORL',\n","        'nickname': 'Magic',\n","        'yearfounded': '1989',\n","        'city': 'Orlando',\n","        'arena': 'Kia Center',\n","        'arenacapacity': '20000',\n","        'owner': 'RDV Sports, Inc.',\n","        'generalmanager': 'Anthony Parker',\n","        'headcoach': 'Jamahl Mosley',\n","        'dleagueaffiliation': 'Osceola Magic y Lakeland Magic',\n","        'facebook': 'https://web.facebook.com/OrlandoMagic/',\n","        'instagram': 'https://www.instagram.com/orlandomagic/',\n","        'twitter': 'https://x.com/OrlandoMagic'\n","    }\n","]\n","df_team_details = pd.concat([df_team_details, pd.DataFrame(nuevos_registros)], ignore_index=True)\n","\n","df_team_details.loc[df_team_details[\"nickname\"] == \"Nuggets\", \"arenacapacity\"] = 21000\n","df_team_details.loc[df_team_details[\"nickname\"] == \"Warriors\", \"arenacapacity\"] = 18064\n","df_team_details.loc[df_team_details[\"nickname\"] == \"Nets\", \"arenacapacity\"] = 19000\n","df_team_details.loc[df_team_details[\"nickname\"] == \"76ers\", \"arenacapacity\"] = 21000\n","df_team_details.loc[df_team_details[\"nickname\"] == \"Suns\", \"arenacapacity\"] = 18422\n","df_team_details.loc[df_team_details[\"nickname\"] == \"Thunder\", \"arenacapacity\"] = 18203\n","df_team_details.loc[df_team_details[\"nickname\"] == \"Raptors\", \"arenacapacity\"] = 19800\n","df_team_details.loc[df_team_details[\"nickname\"] == \"Jazz\", \"arenacapacity\"] = 20000\n","df_team_details.loc[df_team_details[\"nickname\"] == \"Pistons\", \"arenacapacity\"] = 20491\n","df_team_details.loc[df_team_details[\"nickname\"] == \"Raptors\", \"headcoach\"] = 'Darko Rajaković'\n","\n","#df_team_details= df_team_details.sort_values(by='team_id').reset_index()\n","\n","#crear una nueva columna en team_details\n","equipos_este = [\"BOS\", \"BKN\", \"NYK\", \"PHI\", \"TOR\", \"ATL\", \"CHA\", \"MIA\", \"ORL\", \"WAS\", \"CHI\", \"CLE\", \"DET\", \"IND\", \"MIL\"]\n","\n","df_team_details[\"Conferencia\"] = df_team_details[\"abbreviation\"].apply(lambda x: \"East\" if x in equipos_este else \"West\")\n","\n","df_team_details['full_name'] = df_team_details['city'] + ' ' + df_team_details['nickname']\n","\n","\n","# filtrar los datos de 'All Stars' y 'pretemporada' game\n","\n","df_game = df_game[~df_game['season_type'].isin(['All-Star', 'All Star', 'Pre Season'])]\n","\n","# filtro de publico por maximo game_info\n","df_game_info = df_game_info[df_game_info['attendance'] <= 21711]\n","\n","\n","#limpios\n","df_game.to_csv('/df_game.csv', index=False)\n","df_team_details.to_csv('/df_team_details.csv', index=False)\n","df_other_stats.to_csv('/df_other_stats.csv', index=False)\n","df_line.to_csv('/df_line.csv', index=False)\n","df_common_player.to_csv('/df_common_player.csv', index=False)\n","df_draft_history.to_csv('/df_draft_history.csv', index=False)\n","df_game_info.to_csv('/df_game_info.csv', index=False)\n","\n","#Carga\n","\n","# Parámetros de conexión\n","\n","\n","# Configurar conexión a SQL Server\n","server = 'server-sql-grupo1.database.windows.net'\n","database = 'NBA'\n","username = 'Admon'\n","password = 'Password.Server1'\n","\n","# Crear el engine usando pymssql\n","connection_string = f\"mssql+pymssql://{username}:{password}@{server}/{database}\"\n","engine = create_engine(connection_string)\n","\n","# Diccionario de DataFrames (asegúrate de definirlos previamente)\n","dfs_to_load = {\n","    'df_game': df_game,\n","    'df_team_details': df_team_details,\n","    'df_other_stats': df_other_stats,\n","    'df_line': df_line,\n","    'df_common_player': df_common_player,\n","    'df_draft_history': df_draft_history,\n","    'df_game_info': df_game_info,\n","    'df_play_by_play': df_play_by_play\n","}\n","\n","# Cargar los DataFrames en la base de datos\n","for table_name, df in dfs_to_load.items():\n","    df.to_sql(table_name, con=engine, if_exists='replace', index=False)\n","    print(f\"Tabla '{table_name}' cargada correctamente.\")\n","\n"],"metadata":{"colab":{"base_uri":"https://localhost:8080/"},"id":"oTKVbxuQp5_B","outputId":"5666b0f4-614b-4934-fea7-d3433c99c878"},"execution_count":null,"outputs":[{"metadata":{"tags":null},"name":"stdout","output_type":"stream","text":["Drive already mounted at /content/drive; to attempt to forcibly remount, call drive.mount(\"/content/drive\", force_remount=True).\n","mv: cannot stat 'kaggle.json': No such file or directory\n","chmod: cannot access '/root/.kaggle/kaggle.json': No such file or directory\n","Warning: Looks like you're using an outdated API Version, please consider updating (server 1.7.4 / client 1.6.17)\n","Dataset URL: https://www.kaggle.com/datasets/wyattowalsh/basketball\n","License(s): CC-BY-SA-4.0\n","Downloading basketball.zip to /content\n"," 96% 672M/697M [00:02<00:00, 272MB/s]\n","100% 697M/697M [00:03<00:00, 241MB/s]\n","Tabla 'df_game' cargada correctamente.\n","Tabla 'df_team_details' cargada correctamente.\n","Tabla 'df_other_stats' cargada correctamente.\n","Tabla 'df_line' cargada correctamente.\n","Tabla 'df_common_player' cargada correctamente.\n","Tabla 'df_draft_history' cargada correctamente.\n","Tabla 'df_game_info' cargada correctamente.\n"]}]}]}