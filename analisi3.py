import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import pymysql
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score

# Connessione al database
connection = pymysql.connect(
    host='127.0.0.1',
    user='root',
    password='root',
    database='fiumesicuro'
)

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

# Creare un DataFrame Pandas
df = pd.read_sql(query, connection)
connection.close()

# Rimuovere valori nulli
df = df.dropna(subset=['livello_idro', 'temperatura', 'precipitazione_1h'])

# Aggiungere Lagged Features
lag = 1  # Definisci il numero di lag
df['livello_idro_lag'] = df['livello_idro'].shift(lag)
df['temperatura_lag'] = df['temperatura'].shift(lag)
df['precipitazione_lag'] = df['precipitazione_1h'].shift(lag)

# Rimuovere righe con valori nulli a causa del lag
df = df.dropna()

# Visualizzare i dati
print("Dataset con Lagged Features:")
print(df.head())

# Preparazione dei dati per il machine learning
X = df[['temperatura', 'precipitazione_1h', 'temperatura_lag', 'precipitazione_lag']]
y = df['livello_idro']

# Divisione dei dati in addestramento e test
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Definire il modello di Random Forest
model = RandomForestRegressor(random_state=42)
model.fit(X_train, y_train)

# Valutazione del modello
y_pred = model.predict(X_test)
mse = mean_squared_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)
print(f"Mean Squared Error: {mse}, R^2 Score: {r2}")

# Visualizzare previsioni vs valori reali
plt.figure(figsize=(10, 6))
plt.scatter(y_test, y_pred, alpha=0.6, color='blue')
plt.plot([y.min(), y.max()], [y.min(), y.max()], 'r--')  # Linea di perfetta previsione
plt.xlabel("Valori Reali di Livello Idrometrico")
plt.ylabel("Valori Predetti di Livello Idrometrico")
plt.title("Predizioni vs Valori Reali con Lagged Features")
plt.show()
