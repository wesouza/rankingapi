import logging
import requests
import pandas as pd
from flask import Flask, jsonify
from io import BytesIO
from flask_cors import CORS
import mysql.connector
from mysql.connector import Error

# Configuração do logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

google_sheets_xlsx_url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTcKr_yg8RmmOJq8nT11rJWVIpUG89tT90-GHPsZML72Dcws_3nsPHxhNinumM3hYKT32He_qN_OOE3/pub?output=xlsx'

db_config = {
    'host': '177.234.152.114',
    'user': 'vox2youcom_apiranking',
    'password': 'GNjb4SCwLR4Y',
    'database': 'vox2youcom_apiranking'
}

def get_google_sheets_data():
    try:
        logger.debug("Buscando dados da planilha...")
        response = requests.get(google_sheets_xlsx_url)
        logger.debug(f"Status da requisição: {response.status_code}")
        response.raise_for_status()
        
        logger.debug("Planilha baixada com sucesso. Processando...")
        df = pd.read_excel(BytesIO(response.content), engine='openpyxl')
        logger.debug(f"Dados lidos da planilha: {df.head()}")

        if 'data_ultima_atualizacao' in df.columns:
            df['data_ultima_atualizacao'] = pd.to_datetime(df['data_ultima_atualizacao'], format='%d/%m/%Y', errors='coerce')
        else:
            df['data_ultima_atualizacao'] = pd.NaT

        df.dropna(subset=['nome', 'patente', 'unidade', 'categoria', 'pontos', 'foto', 'data_ultima_atualizacao'], inplace=True)

        return df
    except requests.RequestException as e:
        logger.error(f"Erro ao fazer requisição à planilha: {e}")
    except Exception as e:
        logger.error(f"Erro ao processar dados da planilha: {e}")

    return None

def save_data_to_mysql(df):
    try:
        logger.debug("Conectando ao banco de dados...")
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            cursor = connection.cursor()

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS ranking (
                id INT AUTO_INCREMENT PRIMARY KEY,
                categoria VARCHAR(255),
                nome VARCHAR(255),
                patente VARCHAR(255),
                unidade VARCHAR(255),
                foto TEXT,
                pontos INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                data_ultima_atualizacao TIMESTAMP NULL,
                UNIQUE KEY unique_ranking (categoria, nome)
            )
            """)

            insert_query = """
            INSERT INTO ranking (categoria, nome, patente, unidade, foto, pontos, created_at, updated_at, data_ultima_atualizacao)
            VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW(), %s)
            ON DUPLICATE KEY UPDATE
                patente = VALUES(patente),
                unidade = VALUES(unidade),
                foto = VALUES(foto),
                pontos = VALUES(pontos),
                updated_at = NOW(),
                data_ultima_atualizacao = VALUES(data_ultima_atualizacao)
            """

            for index, row in df.iterrows():
                data_ultima_atualizacao = row['data_ultima_atualizacao'].strftime('%Y-%m-%d %H:%M:%S') if pd.notna(row['data_ultima_atualizacao']) else None
                cursor.execute(insert_query, (
                    row['categoria'], row['nome'], row['patente'], 
                    row['unidade'], row['foto'], row['pontos'],
                    data_ultima_atualizacao
                ))

            connection.commit()
            logger.debug("Dados salvos no banco de dados com sucesso.")
    except Error as e:
        logger.error(f"Erro ao conectar ao MySQL: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

@app.route('/api/ranking', methods=['GET'])
def get_ranking():
    df = get_google_sheets_data()
    
    if df is not None:
        save_data_to_mysql(df)
        
        grouped_data = {}
        for _, row in df.iterrows():
            category = row['categoria']
            if category not in grouped_data:
                grouped_data[category] = []
            grouped_data[category].append(row.to_dict())
        
        response = jsonify(grouped_data)
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        response.headers['Accept'] = '*/*'
        return response
    else:
        logger.error("Erro ao buscar dados da planilha.")
        return jsonify({"error": "Erro ao buscar dados da planilha."}), 500

if __name__ == '__main__':
    app.run(debug=True)
