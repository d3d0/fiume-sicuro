from flask import Flask, render_template
import requests

app = Flask(__name__)

@app.route('/')
def home():
    # Endpoint da cui vuoi leggere i dati
    api_url = 'https://apps.arpae.it/REST/meteo_osservati?where={"anagrafica.variabili":"livello_idro"}&projection={"dati.20241022":1,"dati.20241021":1,"dati.20241020":1,"anagrafica":1}&max_results=1000'
    response = requests.get(api_url)
    
    # Controlla se la richiesta è stata effettuata con successo
    if response.status_code == 200:
        print(response.json())
        data = response.json()
    else:
        data = {"error": "Impossibile ottenere i dati"}

    # Mostra i dati nella pagina web
    # return render_template('index.html', data=data)
    return render_template('select.html', data=data, selected_date=selected_date)


if __name__ == '__main__':
    app.run(debug=True)

