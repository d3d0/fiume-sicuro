import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from sklearn.preprocessing import MinMaxScaler
import logging

# Configurazione del logger
logging.basicConfig(level=logging.DEBUG)  # Imposta il livello di logging
logger = logging.getLogger(__name__)

# Estrarre i dati dal database
def load_data(cursor):
    logger.debug("Inizio del caricamento dei dati dal database.")
    query = """
    SELECT data_ora_rilevazione, livello_idro, temperatura
    FROM vista_livello_temperatura
    WHERE livello_idro IS NOT NULL AND temperatura IS NOT NULL
    ORDER BY data_ora_rilevazione ASC;
    """
    cursor.execute(query)
    data = cursor.fetchall()
    
    if not data:
        logger.warning("Nessun dato trovato per la query.")
    
    df = pd.DataFrame(data, columns=['data_ora_rilevazione', 'livello_idro', 'temperatura'])
    logger.debug(f"Dati caricati: {df.head()}")
    return df

# Pre-processamento
def preprocess_data(df):
    logger.debug("Inizio del pre-processamento dei dati.")
    scaler = MinMaxScaler(feature_range=(0, 1))
    df[['livello_idro', 'temperatura']] = scaler.fit_transform(df[['livello_idro', 'temperatura']])
    logger.debug("Dati normalizzati.")
    
    X, y = [], []
    time_step = 10  # Definisce la finestra temporale
    for i in range(len(df) - time_step - 1):
        X.append(df[['livello_idro', 'temperatura']].iloc[i:i + time_step].values)
        y.append(df['livello_idro'].iloc[i + time_step])
    
    X, y = np.array(X), np.array(y)
    logger.debug(f"Dimensione X: {X.shape}, Dimensione y: {y.shape}")
    return X, y, scaler

# Creazione del modello LSTM
def create_model(input_shape):
    logger.debug("Creazione del modello LSTM.")
    model = Sequential()
    model.add(LSTM(units=50, return_sequences=True, input_shape=input_shape))
    model.add(LSTM(units=50))
    model.add(Dense(units=1))
    model.compile(optimizer='adam', loss='mean_squared_error')
    logger.debug("Modello compilato.")
    return model

# Addestramento del modello
def train_model(model, X_train, y_train):
    logger.debug("Inizio dell'addestramento del modello.")
    model.fit(X_train, y_train, epochs=100, batch_size=32, validation_split=0.2)
    logger.debug("Modello addestrato.")
    return model

# Funzione per effettuare previsioni
# def make_predictions(model, data, scaler):
#     predictions = model.predict(data)
#     predictions = scaler.inverse_transform(predictions)
#     return predictions

# Funzione per effettuare previsioni
def make_predictions(model, data, scaler):
    predictions = model.predict(data)
    # Dobbiamo creare un array per le previsioni con forma (721, 2)
    # Qui assumiamo che la seconda variabile sia impostata su un valore costante (puoi adattare come necessario)
    dummy_temperature = np.full(predictions.shape, scaler.data_min_[1])  # Usa un valore costante o calcolato
    predictions_with_temperature = np.hstack((predictions, dummy_temperature))
    predictions_inverse = scaler.inverse_transform(predictions_with_temperature)
    return predictions_inverse[:, 0]  # Ritorna solo il livello_idro

# Funzione per visualizzare i risultati
def plot_results(real_data, predicted_data):
    plt.figure(figsize=(12, 6))
    plt.plot(real_data, color='blue', label='Dati Reali')
    plt.plot(predicted_data, color='red', label='Previsioni')
    plt.title('Previsioni dei Livelli dei Fiumi')
    plt.xlabel('Tempo')
    plt.ylabel('Livello Idro')
    plt.legend()
    plt.show()

# Pipeline completa
def prediction_pipeline(cursor):
    logger.debug(">>>>> Inizio della pipeline di previsione.")
    df = load_data(cursor)
    X, y, scaler = preprocess_data(df)
    model = create_model((X.shape[1], X.shape[2]))
    trained_model = train_model(model, X, y)

    # Effettuare previsioni
    predictions = make_predictions(trained_model, X, scaler)
    
    # Visualizzare i risultati
    plot_results(y, predictions)

    logger.debug(">>>>> Pipeline di previsione completata.")
    return trained_model, scaler
