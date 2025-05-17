import os
import csv
import boto3
import pymysql
import tempfile
from datetime import datetime
import logging
from dotenv import load_dotenv

# Configurar logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('ingesta-auth')

# Cargar variables de entorno
load_dotenv()

# Configuración de MySQL
DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'localhost'),
    'user': os.environ.get('DB_USER', 'root'),
    'password': os.environ.get('DB_PASSWORD', ''),
    'database': os.environ.get('DB_NAME', 'auth'),
    'port': int(os.environ.get('DB_PORT', '3306'))
}

# Configuración S3
S3_BUCKET = 'proy-cloud-bucket'
S3_PREFIX = 'Auth'
TIMESTAMP = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

def connect_to_db():
    try:
        conn = pymysql.connect(**DB_CONFIG)
        logger.info("Conexión a MySQL exitosa")
        return conn
    except Exception as e:
        logger.error(f"Error al conectar a MySQL: {e}")
        raise

def extract_data(conn, table_name, columns):
    try:
        with conn.cursor() as cursor:
            query = f"SELECT {', '.join(columns)} FROM {table_name}"
            logger.info(f"Ejecutando query: {query}")
            cursor.execute(query)
            return cursor.fetchall()
    except Exception as e:
        logger.error(f"Error extrayendo datos de {table_name}: {e}")
        raise

def format_data_for_csv(data):
    formatted = []
    for row in data:
        formatted_row = []
        for value in row:
            if isinstance(value, datetime):
                formatted_row.append(value.strftime('%Y-%m-%d %H:%M:%S'))
            else:
                formatted_row.append(value)
        formatted.append(formatted_row)
    return formatted

def save_to_csv(data, columns, file_path):
    try:
        formatted_data = format_data_for_csv(data)
        with open(file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(columns)
            writer.writerows(formatted_data)
        logger.info(f"Datos guardados en {file_path}")
    except Exception as e:
        logger.error(f"Error guardando CSV: {e}")
        raise

def upload_to_s3(file_path, s3_key):
    try:
        s3 = boto3.client('s3')
        s3.upload_file(file_path, S3_BUCKET, s3_key)
        logger.info(f"Archivo subido a s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        logger.error(f"Error subiendo a S3: {e}")
        raise

def process_table(conn, table_name, columns):
    try:
        data = extract_data(conn, table_name, columns)
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
        temp_file.close()
        save_to_csv(data, columns, temp_file.name)
        s3_key = f"{S3_PREFIX}/{table_name}_{TIMESTAMP}.csv"
        upload_to_s3(temp_file.name, s3_key)
        os.unlink(temp_file.name)
        logger.info(f"Tabla {table_name} procesada correctamente")
    except Exception as e:
        logger.error(f"Error procesando {table_name}: {e}")
        raise

def main():
    try:
        logger.info("Iniciando ingesta del microservicio auth")
        conn = connect_to_db()
        process_table(conn, 'users', ['id', 'nombre', 'email', 'password', 'role', 'token', 'created_at'])
        conn.close()
        logger.info("Ingesta completada exitosamente")
    except Exception as e:
        logger.error(f"Error en el proceso principal: {e}")
        exit(1)

if __name__ == '__main__':
    main()
