import mysql.connector
import pandas as pd
from datetime import datetime

def export_misurazioni_to_csv():
    try:
        # Configurazione della connessione al database
        db_config = {
            'host': 'localhost',
            'user': 'your_username',
            'password': 'your_password',
            'database': 'your_database'
        }

        # Stabilisce la connessione
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Query SQL per estrarre i dati
        query = """
        SELECT 
            m.id,
            m.stazione_id,
            m.data_ora_rilevazione,
            m.data_rilevazione,
            m.ora_rilevazione,
            m.tipo_misurazione,
            m.valore,
            s.nome as nome_stazione,
            s.altitudine,
            s.longitude,
            s.latitude,
            s.comune,
            s.provincia,
            s.regione
        FROM misurazioni m
        INNER JOIN stazioni s ON m.stazione_id = s.id
        WHERE s.multifunzione = 1
        ORDER BY m.data_ora_rilevazione DESC
        """

        # Esegue la query e ottiene i risultati
        print("Esecuzione query...")
        df = pd.read_sql(query, conn)

        # Genera il nome del file con timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'misurazioni_multifunzione_{timestamp}.csv'

        # Esporta in CSV
        df.to_csv(filename, index=False, encoding='utf-8')
        print(f"File CSV creato con successo: {filename}")

        # Statistiche base
        print("\nStatistiche:")
        print(f"Numero totale di misurazioni: {len(df)}")
        print(f"Numero di stazioni uniche: {df['stazione_id'].nunique()}")
        print(f"Tipi di misurazione presenti: {', '.join(df['tipo_misurazione'].unique())}")

    except mysql.connector.Error as err:
        print(f"Errore MySQL: {err}")
    except Exception as e:
        print(f"Errore generico: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            print("\nConnessione al database chiusa")

if __name__ == "__main__":
    export_misurazioni_to_csv()