import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
import matplotlib.pyplot as plt
from sklearn.metrics import mean_squared_error
import joblib

# Step 1: Caricamento dei dati dal database
# Sostituisci con la tua connessione per estrarre i dati
import pymysql
connection = pymysql.connect(
    host='127.0.0.1',
    user='root',
    password='root',
    database='fiumesicuro'
)
# query = "SELECT data_ora_rilevazione, livello_idro, temperatura, precipitazione_1h FROM vista_livello_temperatura"

# Chiedere all'utente se analizzare tutte le stazioni o solo una stazione specifica
scelta = input("Vuoi analizzare tutte le stazioni (T) o solo una stazione specifica (S)? ").strip().upper()

if scelta == 'S':
    stazione_id = input("Inserisci l'ID della stazione che vuoi analizzare: ")
    query = f"""
    SELECT stazione_id, nome_stazione, data_ora_rilevazione, livello_idro, temperatura, precipitazione_1h
    FROM vista_livello_temperatura
    WHERE stazione_id = {stazione_id}
    ORDER BY data_ora_rilevazione
    """
else:
    query = """
    SELECT stazione_id, nome_stazione, data_ora_rilevazione, livello_idro, temperatura, precipitazione_1h
    FROM vista_livello_temperatura
    ORDER BY data_ora_rilevazione
    """

df = pd.read_sql(query, connection)
connection.close()

# Step 2: Preprocessing dei dati
df['data_ora_rilevazione'] = pd.to_datetime(df['data_ora_rilevazione'])
df.set_index('data_ora_rilevazione', inplace=True)
data = df[['livello_idro', 'temperatura', 'precipitazione_1h']]

# Normalizzazione dei dati
scaler = MinMaxScaler(feature_range=(0, 1))
data_scaled = scaler.fit_transform(data)

# Salva lo scaler
joblib.dump(scaler, 'scaler.save')

# Creazione delle sequenze per la LSTM
def create_sequences(data, seq_length=60):
    X, y = [], []
    for i in range(seq_length, len(data)):
        X.append(data[i-seq_length:i])
        y.append(data[i, 0])  # Il target è solo il livello idrometrico
    return np.array(X), np.array(y)

seq_length = 60  # Finestra temporale di 60 passi
X, y = create_sequences(data_scaled, seq_length=seq_length)

# Divisione in set di training e test
split = int(0.8 * len(X))
X_train, X_test = X[:split], X[split:]
y_train, y_test = y[:split], y[split:]

# Step 3: Definizione del modello LSTM
model = Sequential()
model.add(LSTM(50, return_sequences=True, input_shape=(X_train.shape[1], X_train.shape[2])))
model.add(LSTM(50, return_sequences=False))
model.add(Dense(25))
model.add(Dense(1))  # Uscita con un valore, il livello idrometrico

model.compile(optimizer='adam', loss='mean_squared_error')
model.summary()

# Step 4: Addestramento del modello
history = model.fit(X_train, y_train, batch_size=32, epochs=20, validation_data=(X_test, y_test))

# Salva il modello completo
model.save("modello_lstm_completo.h5")

# Step 5: Predizione e visualizzazione dei risultati
predicted = model.predict(X_test)
predicted_rescaled = scaler.inverse_transform(np.concatenate([predicted, np.zeros((predicted.shape[0], 2))], axis=1))[:, 0]
y_test_rescaled = scaler.inverse_transform(np.concatenate([y_test.reshape(-1, 1), np.zeros((y_test.shape[0], 2))], axis=1))[:, 0]

# Grafico dei valori reali vs predetti
plt.figure(figsize=(12, 6))
plt.plot(y_test_rescaled, label="Valori Reali")
plt.plot(predicted_rescaled, label="Valori Predetti")
plt.xlabel("Campioni")
plt.ylabel("Livello Idrometrico")
plt.title("Valori Reali vs Predetti (LSTM)")
plt.legend()
plt.show()

# Step 6: Grafico a dispersione Predetti vs Reali
plt.figure(figsize=(6, 6))
plt.scatter(y_test_rescaled, predicted_rescaled, alpha=0.5)
plt.plot([min(y_test_rescaled), max(y_test_rescaled)], [min(y_test_rescaled), max(y_test_rescaled)], 'k--')
plt.xlabel("Valori Reali")
plt.ylabel("Valori Predetti")
plt.title("Grafico a Dispersione Valori Predetti vs Valori Reali (LSTM)")
plt.show()

# Step 7: Distribuzione degli errori
error = y_test_rescaled - predicted_rescaled
plt.figure(figsize=(10, 6))
plt.hist(error, bins=50, alpha=0.7)
plt.xlabel("Errore (Valori Reali - Predetti)")
plt.ylabel("Frequenza")
plt.title("Distribuzione degli Errori (LSTM)")
plt.show()

# Calcolo del RMSE
rmse = np.sqrt(mean_squared_error(y_test_rescaled, predicted_rescaled))
print(f"RMSE: {rmse}")
