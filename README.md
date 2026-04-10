# 🎵 Data Engineer Challenge: ETL Spotify & Grammys

## 📌 Descripción del Proyecto
Este proyecto es un pipeline ETL completo orquestado con **Apache Airflow**. Su objetivo es extraer datos musicales de dos fuentes distintas (un archivo CSV de Spotify y una base de datos de los Grammys), limpiarlos, transformarlos, fusionarlos y finalmente cargarlos en un Data Warehouse modelado en un esquema de estrella para su posterior análisis en Power BI.

## ⚙️ Diseño del DAG de Airflow (`etl_spotify_grammys_workshop`)
El flujo de trabajo se diseñó para ejecutar tareas en paralelo y asegurar la calidad de los datos antes de la fusión.

1. **`extract_spotify_csv` & `extract_grammys_db` (En paralelo):** Extraen los datos crudos de sus fuentes.
2. **`clean_and_validate`:** Aplica reglas de calidad descubiertas durante el EDA (Data Profiling).
3. **`transform_and_merge`:** Normaliza textos y realiza un `INNER JOIN` entre ambos datasets.
4. **`load_to_google_drive_csv` & `load_to_data_warehouse` (En paralelo):** Exporta el resultado final a un archivo plano y construye dinámicamente las tablas en SQLite.

## 🛠️ Decisiones Técnicas y Modelado de Datos

### Transformación y Limpieza
* **Manejo de Nulos:** Se eliminaron las filas sin nombre de artista o canción (al ser campos clave). Se imputaron valores por defecto en columnas como `album_name` para no perder filas valiosas.
* **Estandarización:** Se asumió que los nombres de artistas y canciones podían tener diferencias de formato. Se aplicó `.str.lower().str.strip()` a las llaves de cruce para garantizar un `MERGE` exitoso.

### Diseño del Data Warehouse (Star Schema)
El modelo de datos se diseñó para optimizar consultas analíticas:
* **Dimensiones (Entities):** `dim_track` (detalles de la canción), `dim_artist` (nombres únicos) y `dim_award` (categoría y año del premio).
* **Hechos (Measures):** `fact_performance` (Métricas numéricas como `popularity` y `danceability`, cruzadas con las llaves foráneas).
* **Grano del Modelo:** Una fila en la tabla de hechos representa las métricas de un artista específico asociado a un premio específico.

## 🚀 Instrucciones de Configuración (Setup)
1. **Clonar el repositorio:** `git clone <url-del-repo>`
2. **Preparar los datos:** Crear una carpeta `data/` en la raíz y colocar ahí los archivos originales `spotify_dataset.csv` y `the_grammy_awards.csv`.
3. **Ejecución:** Levantar los servicios de Airflow usando Docker (`docker-compose up -d`) y activar el DAG desde la interfaz web.
