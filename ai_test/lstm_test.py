import numpy as np
import pandas as pd
from tensorflow.keras.models import load_model
from sklearn.preprocessing import MinMaxScaler
import joblib
import matplotlib.pyplot as plt

# Carica il modello salvato
model = load_model('modello_lstm_completo.h5')

# Carica lo scaler
scaler = joblib.load("scaler.save")

# Genera dati casuali per temperatura e precipitazioni
np.random.seed(42)  # per risultati riproducibili
temperature = np.random.uniform(-5, 35, 50)  # 50 campioni di temperatura
precipitazioni = np.random.uniform(0, 10, 50)  # 50 campioni di precipitazioni

# Combina i dati in un DataFrame
nuovi_dati = pd.DataFrame({
    'livello_idro': np.zeros(50),  # Inizialmente a zero
    'temperatura': temperature,
    'precipitazione_1h': precipitazioni
})

# Normalizza i nuovi dati
nuovi_dati_scaled = scaler.transform(nuovi_dati)

# Crea sequenze da utilizzare per il modello LSTM
def create_sequences(data, seq_length=60):
    X = []
    for i in range(seq_length, len(data)):
        X.append(data[i-seq_length:i])
    return np.array(X)

# Creare sequenze con i nuovi dati
seq_length = 30
if len(nuovi_dati_scaled) >= seq_length:
    nuovi_dati_sequenze = create_sequences(nuovi_dati_scaled, seq_length)

    # Effettua la previsione
    predizione = model.predict(nuovi_dati_sequenze)

    # Inverso dello scaling per riportare i valori alla scala originale
    predizione_inversa = scaler.inverse_transform(np.concatenate((predizione, np.zeros((predizione.shape[0], 2))), axis=1))[:, 0]

    print("Livelli idrometrici predetti:", predizione_inversa)

    # Grafico dei risultati
    plt.figure(figsize=(12, 6))
    plt.plot(predizione_inversa, label="Livelli Idrometrici Predetti", color='blue')
    plt.title("Predizioni dei Livelli Idrometrici")
    plt.xlabel("Campioni")
    plt.ylabel("Livello Idrometrico")
    plt.legend()
    plt.grid()
    plt.show()

    # Otteniamo le temperature e le precipitazioni corrispondenti all'output
    temperature_corresponding = temperature[seq_length:]  # Temperature corrispondenti
    precipitazioni_corresponding = precipitazioni[seq_length:]  # Precipitazioni corrispondenti

    # Grafico a dispersione
    plt.figure(figsize=(12, 6))
    plt.scatter(temperature_corresponding, predizione_inversa, color='blue', label='Predizioni dei Livelli Idrometrici')
    plt.title("Livelli Idrometrici in Base alla Temperatura")
    plt.xlabel("Temperatura (°C)")
    plt.ylabel("Livello Idrometrico")
    plt.legend()
    plt.grid()
    plt.show()

    # Grafico a dispersione per precipitazioni
    plt.figure(figsize=(12, 6))
    plt.scatter(precipitazioni_corresponding, predizione_inversa, color='green', label='Predizioni dei Livelli Idrometrici')
    plt.title("Livelli Idrometrici in Base alle Precipitazioni")
    plt.xlabel("Precipitazioni (mm)")
    plt.ylabel("Livello Idrometrico")
    plt.legend()
    plt.grid()
    plt.show()
else:
    print("Non ci sono abbastanza dati per creare sequenze.")
