from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import sqlite3
import os

# Rutas de los archivos en Docker
DATA_DIR = '/opt/airflow/data'
SPOTIFY_CSV = f'{DATA_DIR}/spotify_dataset.csv'
GRAMMYS_CSV = f'{DATA_DIR}/the_grammy_awards.csv'
GRAMMYS_DB = f'{DATA_DIR}/grammys.db'
CLEAN_SPOTIFY = f'{DATA_DIR}/clean_spotify.csv'
CLEAN_GRAMMYS = f'{DATA_DIR}/clean_grammys.csv'
MERGED_CSV = f'{DATA_DIR}/final_dataset_spotify_grammys.csv'
DW_DB = f'{DATA_DIR}/data_warehouse.db'

default_args = {
    'owner': 'data_engineer_student',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# TAREAS DE EXTRACCIÓN 
def extract_spotify():
    """Valida que el archivo CSV de Spotify exista y se pueda leer."""
    df = pd.read_csv(SPOTIFY_CSV)
    print(f"Spotify extraído: {df.shape[0]} filas.")

def extract_grammys_db():
    """Simula la creación de la DB de Grammys y extrae los datos."""
    df_raw = pd.read_csv(GRAMMYS_CSV)
    conn = sqlite3.connect(GRAMMYS_DB)
    df_raw.to_sql('grammys_table', conn, if_exists='replace', index=False)
    
    df_extracted = pd.read_sql_query("SELECT * FROM grammys_table", conn)
    conn.close()
    print(f"Grammys extraídos de la DB: {df_extracted.shape[0]} filas.")

# TAREA DE LIMPIEZA Y VALIDACIÓN (DATA QUALITY) 
def clean_and_validate():
    """Aplica las reglas de calidad descubiertas en el EDA."""
    spotify_df = pd.read_csv(SPOTIFY_CSV)
    conn = sqlite3.connect(GRAMMYS_DB)
    grammys_df = pd.read_sql_query("SELECT * FROM grammys_table", conn)
    conn.close()

    # Validaciones y Limpieza
    spotify_df = spotify_df.drop(columns=['Unnamed: 0'], errors='ignore')
    spotify_df = spotify_df.dropna(subset=['artists', 'track_name'])
    spotify_df['album_name'] = spotify_df['album_name'].fillna('Álbum Desconocido')
    
    grammys_df = grammys_df.dropna(subset=['artist', 'nominee'])
    grammys_df['workers'] = grammys_df['workers'].fillna('Sin registro')
    grammys_df['img'] = grammys_df['img'].fillna('Sin imagen')

    spotify_df.to_csv(CLEAN_SPOTIFY, index=False)
    grammys_df.to_csv(CLEAN_GRAMMYS, index=False)
    print("Validación y limpieza completada.")

# TAREA DE TRANSFORMACIÓN Y FUSIÓN 
def transform_and_merge():
    """Estandariza los esquemas y fusiona los datasets."""
    spotify_df = pd.read_csv(CLEAN_SPOTIFY)
    grammys_df = pd.read_csv(CLEAN_GRAMMYS)

    # Transformación
    spotify_df['artists_clean'] = spotify_df['artists'].str.lower().str.strip()
    spotify_df['track_name_clean'] = spotify_df['track_name'].str.lower().str.strip()
    grammys_df['artist_clean'] = grammys_df['artist'].str.lower().str.strip()
    grammys_df['nominee_clean'] = grammys_df['nominee'].str.lower().str.strip()

    # Fusión
    merged_df = pd.merge(
        spotify_df, grammys_df, 
        left_on=['artists_clean', 'track_name_clean'], 
        right_on=['artist_clean', 'nominee_clean'], 
        how='inner' 
    )
    merged_df = merged_df.drop(columns=['artists_clean', 'track_name_clean', 'artist_clean', 'nominee_clean'])
    merged_df.to_csv(MERGED_CSV, index=False)
    print("Transformación y fusión completadas exitosamente.")

# CARGA 
def load_to_csv():
    """Carga el archivo CSV (Requisito para Google Drive)."""
    if os.path.exists(MERGED_CSV):
        print("Archivo CSV listo para ser subido a Google Drive.")

def load_to_dw():
    """Diseña y carga el Modelo de Estrella en el Data Warehouse."""
    merged_df = pd.read_csv(MERGED_CSV)
    
    dim_track = merged_df[['track_id', 'track_name', 'album_name', 'track_genre', 'duration_ms', 'explicit']].drop_duplicates()
    dim_artist = merged_df[['artists']].drop_duplicates().reset_index(drop=True)
    dim_artist['artist_id'] = dim_artist.index + 1
    dim_award = merged_df[['title', 'year', 'category', 'winner']].drop_duplicates().reset_index(drop=True)
    dim_award['award_id'] = dim_award.index + 1
    
    fact_metrics = merged_df[['track_id', 'artists', 'title', 'year', 'popularity', 'danceability', 'energy', 'tempo']]
    fact_metrics = pd.merge(fact_metrics, dim_artist, on='artists', how='left')
    fact_metrics = pd.merge(fact_metrics, dim_award, on=['title', 'year'], how='left')
    fact_table = fact_metrics[['track_id', 'artist_id', 'award_id', 'popularity', 'danceability', 'energy', 'tempo']]

    dw_conn = sqlite3.connect(DW_DB)
    dim_track.to_sql('dim_track', dw_conn, if_exists='replace', index=False)
    dim_artist.to_sql('dim_artist', dw_conn, if_exists='replace', index=False)
    dim_award.to_sql('dim_award', dw_conn, if_exists='replace', index=False)
    fact_table.to_sql('fact_performance', dw_conn, if_exists='replace', index=False)
    dw_conn.close()
    print("Carga al Data Warehouse exitosa.")

#  DEFINICIÓN DEL DAG Y DEPENDENCIAS 
with DAG('etl_spotify_grammys_workshop', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    t1 = PythonOperator(task_id='extract_spotify_csv', python_callable=extract_spotify)
    t2 = PythonOperator(task_id='extract_grammys_db', python_callable=extract_grammys_db)
    t3 = PythonOperator(task_id='clean_and_validate', python_callable=clean_and_validate)
    t4 = PythonOperator(task_id='transform_and_merge', python_callable=transform_and_merge)
    t5 = PythonOperator(task_id='load_to_google_drive_csv', python_callable=load_to_csv)
    t6 = PythonOperator(task_id='load_to_data_warehouse', python_callable=load_to_dw)

    # Orden de ejecución
    [t1, t2] >> t3 >> t4 >> [t5, t6]
    