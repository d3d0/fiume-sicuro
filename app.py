from flask import Flask, render_template, request
import requests
from datetime import datetime, timedelta
import pymysql.cursors


app = Flask(__name__)

def save_data_to_db(data, selected_date):
    connection = pymysql.connect(host='localhost',
                             user='root',
                             password='root',
                             database='fiumi',
                             cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            for item in data['_items']:
                nome_stazione = item['anagrafica']['nome']
                livello = item['dati'].get(selected_date, None)
                
                # Se livello è None, usa un valore predefinito (ad es. 0) o salta l'inserimento
                if livello is not None:
                    ultima_ora = list(livello.keys())[-1]  # Prende l'ultima chiave
                    ultimo_valore = livello[ultima_ora]['livello_idro']    # Prende il valore associato
                    sql = "INSERT INTO `livello_idro` (`nome_stazione`, `data`, `valore`) VALUES (%s, %s, %s)" # Inserisci i dati nel database
                    cursor.execute(sql, (nome_stazione, selected_date, ultimo_valore))

        connection.commit()  # Commit della transazione
    finally:
        connection.close()

@app.route('/')
def home():
    oggi = datetime.now().strftime('%d/%m/%Y')  # Formato YYYYMMDD
    ieri = (datetime.now() - timedelta(days=1)).strftime('%d/%m/%Y')  # Formato DD/MM/YYYY
    altroieri = (datetime.now() - timedelta(days=2)).strftime('%d/%m/%Y')  # Formato DD/MM/YYYY

    today = datetime.now().strftime('%Y%m%d')  # Formato YYYYMMDD
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')  # Formato DD/MM/YYYY
    twodaysbefore = (datetime.now() - timedelta(days=2)).strftime('%Y%m%d')  # Formato DD/MM/YYYY
    selected_date = request.args.get('date', today)  # Data odierna come predefinita
    selected_station = request.args.get('station')  # Aggiunto per la stazione selezionata

    api_url = f'https://apps.arpae.it/REST/meteo_osservati?where={{"anagrafica.variabili":"livello_idro"}}&projection={{"dati.{selected_date}":1,"anagrafica":1}}&max_results=1000'
    response = requests.get(api_url)
    
    if response.status_code == 200:
        data = response.json()  # Dati JSON
        
        # stations = data['_items']  # Salva le stazioni
        stations = sorted(data['_items'], key=lambda x: x['anagrafica']['nome'])  # Ordinamento alfabetico

        for item in data['_items']:

            # Verifica se 'sensori' contiene dati
            sensori = item['anagrafica'].get('sensori', {})
            if 'livello_idro' in sensori and 'soglie' in sensori['livello_idro']:
                soglie = sensori['livello_idro'].get('soglie', [])
                soglie_filtrate = [s for s in soglie if s is not None]

                if soglie_filtrate:
                    livello_massimo_soglie = max(soglie_filtrate)
                    print(f"Il valore massimo delle soglie è: {livello_massimo_soglie}")
                else:
                    livello_massimo_soglie = None
                    print("Nessun valore valido nelle soglie")
            else:
                livello_massimo_soglie = None
                print("Sensori o soglie non disponibili")
            item['livello_massimo_soglie'] = livello_massimo_soglie

            # Ottiene il valore idrometrico più recente
            livello = item['dati'].get(selected_date, {})
            if livello:
                ultima_ora = list(livello.keys())[-1]
                item['ultimo_valore'] = livello[ultima_ora]['livello_idro']

                print(f"valore ---> {item['ultimo_valore']}")
                print(f"soglia ---> {item['livello_massimo_soglie']}")

                # Controlla se l'ultimo valore supera la soglia massima
                if item['ultimo_valore'] is not None and item['livello_massimo_soglie'] is not None:
                    if item['ultimo_valore'] > item['livello_massimo_soglie']:
                        item['colore_valore'] = 'bg-danger'  # Colore rosso
                    else:
                        item['colore_valore'] = 'bg-success'  # Colore verde
                else:
                    item['colore_valore'] = 'bg-secondary'
            else:
                item['ultimo_valore'] = None
                item['colore_valore'] = 'bg-secondary'  # Colore normale se non ci sono dati

        # save_data_to_db(data, selected_date)  # Salva i dati nel DB
    else:
        data = {"error": "Impossibile ottenere i dati"}
        stations = []

    return render_template('table.html', data=data, selected_date=selected_date, stations=stations,
                           selected_station=selected_station, today=today, yesterday=yesterday, twodaysbefore=twodaysbefore, 
                           oggi=oggi, ieri=ieri, altroieri=altroieri)

if __name__ == '__main__':
    app.run(debug=True)
