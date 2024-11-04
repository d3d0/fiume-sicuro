import json
import mysql.connector
from datetime import datetime, date
import logging
import requests
from typing import Dict, Any
import os
from dotenv import load_dotenv
import signal
import sys
import schedule
import time

# Carica le variabili d'ambiente dal file .env
load_dotenv()

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/arpae_data_loader.log'),
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
            "max_results": 100000
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
            comune, provincia, regione, multifunzione
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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
            regione = VALUES(regione),
            multifunzione = VALUES(multifunzione)
        """
        ana = station_data['anagrafica']

        # flag for multi-variable stations.
        multifunzione = 1 if len(ana['variabili']) > 1 else 0

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
            ana['regione'],
            multifunzione
        )
        
        self.cursor.execute(sql, values)
        self.connection.commit()
        logger.info(f"> Stazione: {ana['nome']} (ID: {station_data['_id']}) inserita/aggiornata")

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
            logger.info(f">>> Sensore: {tipo_variabile} per stazione {station_id} inserito/aggiornato")

    def insert_measurements(self, station_id: str, measurements_data: Dict[str, Any], date_str: str) -> None:
        
        # Controlla se la stazione ha multifunzione impostato a 1
        sql_multifunzione_check = """
        SELECT multifunzione 
        FROM stazioni 
        WHERE id = %s;
        """
        self.cursor.execute(sql_multifunzione_check, (station_id,))
        result = self.cursor.fetchone()
        
        # Procede solo se la stazione ha multifunzione = 1
        # if result is None or result['multifunzione'] != 1:
        #     logger.warning(f"La stazione {station_id} non è multifunzione!")
        #     logger.info("-" * 25)
        #     return
        
        # Controlla se measurements_data contiene 'livello_idro' e 'temperatura_istantanea_2m'
        # if not all(key in measurements_data.get(date_str, {}).get(next(iter(measurements_data[date_str])), {}) for key in ['livello_idro', 'temperatura_istantanea_2m']):
        #     logger.warning(f"Misurazioni mancanti: 'livello_idro' e/o 'temperatura_istantanea_2m' per la data {date_str} nella stazione {station_id}.")
        #     logger.info("-" * 25)
        #     return

        # controllo su aggiornamento
        update = False
        
        if date_str not in measurements_data:
            logger.warning(f"Nessun dato disponibile per la data {date_str} nella stazione {station_id}")
            return

        data_formattata_singola = datetime.strptime(date_str, '%Y%m%d').date()

        for ora, misurazioni in measurements_data[date_str].items():
            ora_formattata_singola = f"{ora[:2]}:{ora[2:]}:00"
            ora_formattata = f"{ora[:2]}:{ora[2:]}:00"
            data_formattata = datetime.strptime(f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}", '%Y-%m-%d').date()
            data_ora_rilevazione = datetime.combine(data_formattata, datetime.strptime(ora_formattata, '%H:%M:%S').time())

            for tipo_mis, valore in misurazioni.items():
                sql = """
                SELECT COUNT(*) AS conteggio
                FROM misurazioni
                WHERE stazione_id = %s AND data_ora_rilevazione = %s AND tipo_misurazione = %s;
                """
                self.cursor.execute(sql, (station_id, data_ora_rilevazione, tipo_mis))

                result = self.cursor.fetchone()

                # Verifica se result non è None
                if result is not None:
                    count = result['conteggio']  # Estrai il conteggio
                    if count > 0:
                        # logger.info(f"Record già presente per la stazione {station_id} con data/ora {data_ora_rilevazione} e tipo {tipo_mis}.")
                        sql = """
                        UPDATE misurazioni
                        SET valore = %s
                        WHERE stazione_id = %s AND data_ora_rilevazione = %s AND tipo_misurazione = %s;
                        """
                        self.cursor.execute(sql, (valore, station_id, data_ora_rilevazione, tipo_mis))
                        update = True
                    else:
                        # logger.info(f"Nuova misurazione da inserire per la stazione {station_id}.")
                        sql = """
                        INSERT INTO misurazioni (
                            stazione_id, data_ora_rilevazione, data_rilevazione, ora_rilevazione, tipo_misurazione, valore
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                        """
                        values = (
                            station_id,
                            data_ora_rilevazione,
                            data_formattata_singola,
                            ora_formattata_singola,
                            tipo_mis,
                            valore
                        )
                        self.cursor.execute(sql, values)
                        update = False
                else:
                    logger.warning("Nessun risultato dalla query COUNT.")

        self.connection.commit()
        if update:
            logger.info(f">>>>>>>>>> Misurazioni per stazione {station_id} del {date_str} aggiornate")
        else:
            logger.info(f">>>>>>>>>> Misurazioni per stazione {station_id} del {date_str} inserite")
        logger.info("-" * 25)

    def insert_measurements_OLD(self, station_id: str, measurements_data: Dict[str, Any], date_str: str) -> None:
        """Inserisce le misurazioni."""
        if date_str not in measurements_data:
            logger.warning(f"Nessun dato disponibile per la data {date_str} nella stazione {station_id}")
            return

        sql = """
        INSERT INTO misurazioni (
            stazione_id, data_ora_rilevazione, data_rilevazione, ora_rilevazione, tipo_misurazione, valore
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE valore = VALUES(valore)
        """

        data_formattata_singola = datetime.strptime(date_str, '%Y%m%d').date()

        for ora, misurazioni in measurements_data[date_str].items():
            ora_formattata_singola = f"{ora[:2]}:{ora[2:]}:00"
            ora_formattata = f"{ora[:2]}:{ora[2:]}:00"
            data_formattata = datetime.strptime(f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}", '%Y-%m-%d').date()
            data_ora_rilevazione = datetime.combine(data_formattata, datetime.strptime(ora_formattata, '%H:%M:%S').time())

            for tipo_mis, valore in misurazioni.items():
                values = (
                    station_id,
                    data_ora_rilevazione,
                    data_formattata_singola,
                    ora_formattata_singola,
                    tipo_mis,
                    valore
                )

                self.cursor.execute(sql, values)

        self.connection.commit()
        logger.info(f">>>>> Misurazioni per stazione {station_id} del {date_str} inserite/aggiornate")
        logger.info("-" * 25)  # Linea separatrice dopo l'elaborazione

    def process_data(self, selected_date: str) -> None:
        """Elabora i dati dall'API e li inserisce nel database."""
        try:
            # Recupera i dati dall'API
            json_data = self.fetch_data_from_api(selected_date)
            
            # ATTENZIONE: Processa solo la prima stazione
            # if json_data['_items']:
            #     item = json_data['_items'][0]

            # ATTENZIONE: Processa ogni stazione
            if json_data['_items']:
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
        logger.info(f"Database: {os.getenv('DB_NAME', 'fiumesicuro')}")
        logger.info(f"Utente: {os.getenv('DB_USER', 'root')}")
        logger.info("-" * 50)  # Linea separatrice dopo l'elaborazione
        logger.info("")  # Riga vuota alla fine

        # Chiedi all'utente se vuole usare la data odierna o una data specifica
        scelta = input("Vuoi utilizzare la data odierna (O) o specificare una data (S)? ").strip().upper()
        
        if scelta == 'S':
            # Input della data in formato YYYYMMDD
            data_input = input("Inserisci la data (formato YYYYMMDD): ")
            selected_date = data_input
        else:
            # Data odierna
            selected_date = date.today().strftime('%Y%m%d')
        
        logger.info(f"Data selezionata: {selected_date}")

        # Data per cui recuperare i dati (formato YYYYMMDD)
        # selected_date = date.today().strftime('%Y%m%d')
        
        # Inizializza il loader e processa i dati
        loader = ArpaeDataLoader()
        loader.process_data(selected_date)
        
    except Exception as e:
        logger.error(f"Errore durante l'esecuzione: {str(e)}")
    finally:
        if 'loader' in locals():
            loader.close()

def scheduled_data_import():
    while True:
        main()
        logger.info("Elaborazione dei dati completata. Attendo 30 minuti per il prossimo ciclo.")
        logger.info("")  # Riga vuota alla fine
        # time.sleep(1800)  # Attesa di 30 minuti
        time.sleep(5)  # Attesa di 5 minuti

def signal_handler(sig, frame):
    logger.info("Interruzione rilevata. Chiusura in corso...")
    sys.exit(0)

if __name__ == "__main__":
    # main()

    # Configura il gestore di segnali per intercettare l'interruzione da tastiera
    signal.signal(signal.SIGINT, signal_handler)

    # Configura lo scheduler per eseguire la funzione scheduled_data_import ogni 30 minuti
    schedule.every(1).seconds.do(scheduled_data_import)

    while True:
        schedule.run_pending()
        time.sleep(1)

# query di test:
# INSERT INTO `misurazioni` (stazione_id, data_ora_rilevazione, data_rilevazione, ora_rilevazione, tipo_misurazione, valore) 
# VALUES (13040, '2024-10-30 00:00:00', '2024-10-30', '11:30:00', 'livello_idro', 1.63)
# ON DUPLICATE KEY UPDATE valore = VALUES(valore)