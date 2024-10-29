import json
import mysql.connector
from datetime import datetime, date
import logging
import requests
from typing import Dict, Any
import os
from dotenv import load_dotenv

# Carica le variabili d'ambiente dal file .env
load_dotenv()

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('arpae_data_loader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ArpaeDataLoader:
    def __init__(self):
        """Inizializza la connessione al database usando le variabili d'ambiente."""
        try:
            self.connection = mysql.connector.connect(
                host=os.getenv('DB_HOST', '127.0.0.1'),      # Usa IP invece di localhost
                port=int(os.getenv('DB_PORT', '3306')),      # Porta esplicita
                user=os.getenv('DB_USER', 'root'),
                password=os.getenv('DB_PASSWORD', 'root'),
                database=os.getenv('DB_NAME', 'fiumesicuro'),
                charset='utf8mb4',
                collation='utf8mb4_unicode_ci',
                connect_timeout=30  # Timeout di connessione aumentato
            )
            self.cursor = self.connection.cursor(dictionary=True)
            logger.info("Connessione al database stabilita con successo")
        except mysql.connector.Error as err:
            logger.error(f"Errore di connessione al database: {err}")
            raise
        
    def fetch_data_from_api(self, selected_date: str) -> Dict[str, Any]:
        """Recupera i dati dall'API ARPAE."""
        base_url = "https://apps.arpae.it/REST/meteo_osservati"
        
        # Costruzione della query
        where_clause = {"anagrafica.variabili": "livello_idro"}
        projection_clause = {
            f"dati.{selected_date}": 1,
            "anagrafica": 1
        }
        
        # Parametri della query
        params = {
            "where": json.dumps(where_clause),
            "projection": json.dumps(projection_clause),
            "max_results": 1000
        }
        
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()  # Solleva un'eccezione per risposte non 2xx
            return response.json()
            
        except requests.RequestException as e:
            logger.error(f"Errore durante il recupero dei dati dall'API: {str(e)}")
            raise

    def insert_station(self, station_data: Dict[str, Any]) -> None:
        """Inserisce o aggiorna i dati della stazione."""
        sql = """
        INSERT INTO stazioni (
            id, nome, altitudine, longitude, latitude, cod_istat,
            bacino, sottobacino, macroarea, proprietario, gestore,
            comune, provincia, regione
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) ON DUPLICATE KEY UPDATE
            nome = VALUES(nome),
            altitudine = VALUES(altitudine),
            longitude = VALUES(longitude),
            latitude = VALUES(latitude),
            cod_istat = VALUES(cod_istat),
            bacino = VALUES(bacino),
            sottobacino = VALUES(sottobacino),
            macroarea = VALUES(macroarea),
            proprietario = VALUES(proprietario),
            gestore = VALUES(gestore),
            comune = VALUES(comune),
            provincia = VALUES(provincia),
            regione = VALUES(regione)
        """
        ana = station_data['anagrafica']
        values = (
            station_data['_id'],
            ana['nome'],
            ana['altitudine'],
            ana['geometry']['coordinates'][0],
            ana['geometry']['coordinates'][1],
            ana['cod_istat'],
            ana['bacino'],
            ana['sottobacino'],
            ana['macroarea'],
            ana['proprietario'],
            ana['gestore'],
            ana['comune'],
            ana['provincia'],
            ana['regione']
        )
        
        self.cursor.execute(sql, values)
        self.connection.commit()
        logger.info(f"Stazione {ana['nome']} (ID: {station_data['_id']}) inserita/aggiornata")

    def insert_sensors(self, station_id: str, sensors_data: Dict[str, Any]) -> None:
        """Inserisce o aggiorna i dati dei sensori."""
        for tipo_variabile, sensor in sensors_data.items():
            sql = """
            INSERT INTO sensori (
                stazione_id, tipo_variabile, soglia1, soglia2, soglia3,
                bacino, sottobacino, altitudine
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            ) ON DUPLICATE KEY UPDATE
                soglia1 = VALUES(soglia1),
                soglia2 = VALUES(soglia2),
                soglia3 = VALUES(soglia3),
                bacino = VALUES(bacino),
                sottobacino = VALUES(sottobacino),
                altitudine = VALUES(altitudine)
            """
            values = (
                station_id,
                tipo_variabile,
                sensor['soglie'][0],
                sensor['soglie'][1],
                sensor['soglie'][2],
                sensor['bacino'],
                sensor['sottobacino'],
                sensor['altitudine']
            )
            
            self.cursor.execute(sql, values)
            self.connection.commit()
            logger.info(f"Sensore {tipo_variabile} per stazione {station_id} inserito/aggiornato")

    def insert_measurements(self, station_id: str, measurements_data: Dict[str, Any], date_str: str) -> None:
        """Inserisce le misurazioni."""
        if date_str not in measurements_data:
            logger.warning(f"Nessun dato disponibile per la data {date_str} nella stazione {station_id}")
            return

        sql = """
        INSERT INTO misurazioni (
            stazione_id, data_rilevazione, ora_rilevazione,
            tipo_misurazione, valore
        ) VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE valore = VALUES(valore)
        """
        
        data_formattata = datetime.strptime(date_str, '%Y%m%d').date()
        
        for ora, misurazioni in measurements_data[date_str].items():
            ora_formattata = f"{ora[:2]}:{ora[2:]}:00"
            
            for tipo_mis, valore in misurazioni.items():
                values = (
                    station_id,
                    data_formattata,
                    ora_formattata,
                    tipo_mis,
                    valore
                )
                
                self.cursor.execute(sql, values)
                
        self.connection.commit()
        logger.info(f"Misurazioni per stazione {station_id} del {date_str} inserite")

    def process_data(self, selected_date: str) -> None:
        """Elabora i dati dall'API e li inserisce nel database."""
        try:
            # Recupera i dati dall'API
            json_data = self.fetch_data_from_api(selected_date)
            
            # Processa ogni stazione
            for item in json_data['_items']:
                # Inserisce i dati della stazione
                self.insert_station(item)
                
                # Inserisce i dati dei sensori
                if 'sensori' in item['anagrafica']:
                    self.insert_sensors(item['_id'], item['anagrafica']['sensori'])
                
                # Inserisce le misurazioni
                if 'dati' in item:
                    self.insert_measurements(item['_id'], item['dati'], selected_date)
                    
            logger.info(f"Elaborazione dei dati per la data {selected_date} completata con successo")
            
        except Exception as e:
            logger.error(f"Errore durante l'elaborazione dei dati: {str(e)}")
            self.connection.rollback()
            raise

    def close(self):
        """Chiude la connessione al database."""
        self.cursor.close()
        self.connection.close()
        logger.info("Connessione al database chiusa")

def main():
    try:
        # Mostra le configurazioni di connessione (senza password)
        logger.info(f"Tentativo di connessione a: {os.getenv('DB_HOST', '127.0.0.1')}:{os.getenv('DB_PORT', '3306')}")
        logger.info(f"Database: {os.getenv('DB_NAME', 'arpae_db')}")
        logger.info(f"Utente: {os.getenv('DB_USER', 'root')}")
        
        # Data per cui recuperare i dati (formato YYYYMMDD)
        selected_date = date.today().strftime('%Y%m%d')
        
        # Inizializza il loader e processa i dati
        loader = ArpaeDataLoader()
        loader.process_data(selected_date)
        
    except Exception as e:
        logger.error(f"Errore durante l'esecuzione: {str(e)}")
    finally:
        if 'loader' in locals():
            loader.close()

if __name__ == "__main__":
    main()
    