import mysql.connector  # Assicurati di avere questa libreria installata
from machine_learning import prediction_pipeline  # Importa la tua funzione
import logging
import os
from dotenv import load_dotenv

# Carica le variabili d'ambiente dal file .env
load_dotenv()

# Configurazione del logger
logging.basicConfig(level=logging.DEBUG)  # Imposta il livello di logging
logger = logging.getLogger(__name__)

# Sostituisci questi parametri con quelli del tuo database
db_config = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'root',
    'database': 'fiumesicuro',
}

# Connessione al database
try:
    # connection = mysql.connector.connect(**db_config)

    connection = mysql.connector.connect(
                host=os.getenv('DB_HOST', '127.0.0.1'),      # Usa IP invece di localhost
                port=int(os.getenv('DB_PORT', '3306')),      # Porta esplicita
                user=os.getenv('DB_USER', 'root'),
                password=os.getenv('DB_PASSWORD', 'root'),
                database=os.getenv('DB_NAME', 'fiumesicuro'),
                charset='utf8mb4',
                collation='utf8mb4_unicode_ci',
                connect_timeout=30  # Timeout di connessione aumentato
            )
    cursor = connection.cursor(dictionary=True)
    logger.info("Connessione al database stabilita con successo")
    
    cursor = connection.cursor(dictionary=True)
    
    # Esegui la pipeline di previsione
    trained_model, scaler = prediction_pipeline(cursor)
    
    # Chiudi il cursore e la connessione
    cursor.close()
    connection.close()
except mysql.connector.Error as err:
    print(f"Errore: {err}")
