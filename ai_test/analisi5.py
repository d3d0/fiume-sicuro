import pymysql
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from tensorflow.keras.callbacks import EarlyStopping
import matplotlib.pyplot as plt

# === Connessione e Lettura Dati dal Database ===

# Connessione al database MariaDB
connection = pymysql.connect(
    host='127.0.0.1',
    user='root',
    password='root',
    database='fiumesicuro'
)

# Esecuzione della query per ottenere i dati
# query = """
#     SELECT stazione_id, data_ora_rilevazione, livello_idro, temperatura, precipitazione_1h
#     FROM vista_livello_temperatura
#     WHERE livello_idro IS NOT NULL
#     AND temperatura IS NOT NULL
#     AND precipitazione_1h IS NOT NULL
#     """

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

# Lettura dati dal database in un DataFrame Pandas
df = pd.read_sql(query, connection)
connection.close()

# Separazione delle variabili di input (X) e output (y)
X = df[['temperatura', 'precipitazione_1h']]
y = df['livello_idro']

# Suddivisione del dataset in set di addestramento e test
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Normalizzazione dei dati
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# Creazione del modello di rete neurale
model = Sequential()
model.add(Dense(64, activation='relu', input_shape=(X_train.shape[1],)))
model.add(Dense(32, activation='relu'))
model.add(Dense(1))  # Uscita a un solo neurone per la previsione del livello idrometrico

# Compilazione del modello
model.compile(optimizer='adam', loss='mse', metrics=['mae'])

# Early stopping per evitare sovraddestramento
early_stopping = EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)

# Addestramento del modello
history = model.fit(X_train, y_train, validation_data=(X_test, y_test),
                    epochs=100, batch_size=32, callbacks=[early_stopping])

# Valutazione del modello
test_loss, test_mae = model.evaluate(X_test, y_test)
print(f'MAE sul test set: {test_mae}')

# Previsioni sui dati di test
y_pred = model.predict(X_test)

# Visualizzazione dei risultati
# 1. Grafico a Linee dei Valori Reali e Predetti
plt.figure(figsize=(12, 6))
plt.plot(y_test.values, label='Valori Reali')
plt.plot(y_pred, label='Valori Predetti')
plt.xlabel('Campioni')
plt.ylabel('Livello Idrometrico')
plt.legend()
plt.title('Valori Reali vs Predetti')
plt.show()

# 2. Grafico a Dispersione Valori Predetti vs Valori Reali
plt.figure(figsize=(6, 6))
plt.scatter(y_test, y_pred, alpha=0.5)
plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'k--', lw=2)
plt.xlabel('Valori Reali')
plt.ylabel('Valori Predetti')
plt.title('Grafico a Dispersione Valori Predetti vs Valori Reali')
plt.show()

# 3. Distribuzione degli Errori
errors = y_test - y_pred.flatten()
plt.figure(figsize=(10, 6))
plt.hist(errors, bins=30, edgecolor='k', alpha=0.7)
plt.xlabel('Errore')
plt.ylabel('Frequenza')
plt.title('Distribuzione degli Errori (Valori Reali - Valori Predetti)')
plt.show()
