import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import pymysql
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score

# Connessione al database
connection = pymysql.connect(
    host='127.0.0.1',  # Cambia con l'indirizzo del tuo database
    user='root',  # Sostituisci con il tuo nome utente
    password='root',  # Sostituisci con la tua password
    database='fiumesicuro'  # Sostituisci con il nome del tuo database
)

# Chiedere all'utente se analizzare tutte le stazioni o una sola
scelta = input("Vuoi analizzare tutte le stazioni (T) o solo una stazione specifica (S)? ").strip().upper()

# Specifica l'ID o il nome della stazione che desideri filtrare
stazione_id = 3001  # Sostituisci con l'ID della stazione che vuoi

if scelta == 'S':
    # Query per ottenere i dati dalla vista di una STAZIONE
    query = "SELECT stazione_id, nome_stazione, data_ora_rilevazione, livello_idro, temperatura FROM vista_livello_temperatura WHERE stazione_id = %s"
    # Creare un DataFrame Pandas
    df = pd.read_sql(query, connection, params=(stazione_id,))
else:
    # Query per ottenere i dati dalla vista
    query = "SELECT stazione_id, nome_stazione, data_ora_rilevazione, livello_idro, temperatura FROM vista_livello_temperatura"
    # Creare un DataFrame Pandas
    df = pd.read_sql(query, connection)

# Chiudere la connessione
connection.close()

# 1. Visualizzare i dati
print("Dataset:")
print(df)

# Rimuovere eventuali valori nulli
df = df.dropna(subset=['livello_idro', 'temperatura'])

# 2. Distribuzione del livello idrometrico
plt.figure(figsize=(8, 4))
sns.histplot(df['livello_idro'], bins=5, kde=True, color='skyblue')
plt.title("Distribuzione del Livello Idrometrico")
plt.xlabel("Livello Idrometrico (m)")
plt.ylabel("Frequenza")
plt.show()

# 3. Distribuzione della temperatura
plt.figure(figsize=(8, 4))
sns.histplot(df['temperatura'], bins=5, kde=True, color='salmon')
plt.title("Distribuzione della Temperatura")
plt.xlabel("Temperatura (°C)")
plt.ylabel("Frequenza")
plt.show()

# 4. Scatter plot per la correlazione tra livello idrometrico e temperatura
plt.figure(figsize=(6, 4))
sns.scatterplot(x='temperatura', y='livello_idro', data=df, color='purple')
plt.title("Correlazione tra Livello Idrometrico e Temperatura")
plt.xlabel("Temperatura (°C)")
plt.ylabel("Livello Idrometrico (m)")
plt.show()

################################################
################################################

# Preparazione dei dati per il machine learning
X = df[['temperatura']]  # Variabile indipendente
y = df['livello_idro']    # Variabile dipendente

# Divisione del dataset in dati di addestramento e test
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Creazione e addestramento del modello di regressione lineare
model = LinearRegression()
model.fit(X_train, y_train)

# Previsione sui dati di test
y_pred = model.predict(X_test)

# Valutazione del modello
mse = mean_squared_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)
print(f"Mean Squared Error: {mse}")
print(f"R^2 Score: {r2}")

# Visualizzare i risultati della regressione
plt.figure(figsize=(8, 4))
plt.scatter(X_test, y_test, color='blue', label='Dati Reali')
plt.plot(X_test, y_pred, color='red', linewidth=2, label='Previsioni')
plt.title("Regressione Lineare - Livello Idrometrico vs Temperatura")
plt.xlabel("Temperatura (°C)")
plt.ylabel("Livello Idrometrico (m)")
plt.legend()
plt.show()
