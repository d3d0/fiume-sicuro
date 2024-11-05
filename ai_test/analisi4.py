import pymysql
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import xgboost as xgb
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

# === Preprocessing dei Dati ===

# Sostituisci le colonne con quelle che ti servono
X = df[['temperatura', 'precipitazione_1h']]
y = df['livello_idro']

# Suddivisione dei dati in train e test
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# === Addestramento e Predizione con i Modelli ===

# Modello Random Forest
rf_model = RandomForestRegressor(n_estimators=100, random_state=42)
rf_model.fit(X_train, y_train)
rf_predictions = rf_model.predict(X_test)

# Modello XGBoost
xgb_model = xgb.XGBRegressor(objective="reg:squarederror", n_estimators=100, random_state=42)
xgb_model.fit(X_train, y_train)
xgb_predictions = xgb_model.predict(X_test)

# Calcolo RMSE
rf_rmse = mean_squared_error(y_test, rf_predictions, squared=False)
xgb_rmse = mean_squared_error(y_test, xgb_predictions, squared=False)
print(f"RMSE Random Forest: {rf_rmse}")
print(f"RMSE XGBoost: {xgb_rmse}")

# === Visualizzazione ===

# 1. Grafico a Linee dei Valori Reali e Predetti
plt.figure(figsize=(10, 5))
plt.plot(y_test.values, label='Valori Reali', marker='o')
plt.plot(rf_predictions, label='Predetti (Random Forest)', linestyle='--', marker='x')
plt.plot(xgb_predictions, label='Predetti (XGBoost)', linestyle='--', marker='x')
plt.xlabel('Indice Campione')
plt.ylabel('Livello Idrometrico')
plt.title('Confronto Valori Reali e Predetti')
plt.legend()
plt.show()

# 2. Grafico a Dispersione Valori Predetti vs Valori Reali
plt.figure(figsize=(10, 5))
plt.scatter(y_test, rf_predictions, alpha=0.5, label='Random Forest')
plt.scatter(y_test, xgb_predictions, alpha=0.5, label='XGBoost')
plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--')  # Linea y=x
plt.xlabel('Valori Reali')
plt.ylabel('Valori Predetti')
plt.title('Confronto Valori Reali e Predetti')
plt.legend()
plt.show()

# 3. Distribuzione degli Errori
rf_errors = y_test - rf_predictions
xgb_errors = y_test - xgb_predictions

plt.figure(figsize=(10, 5))
plt.hist(rf_errors, bins=20, alpha=0.5, label='Errori Random Forest')
plt.hist(xgb_errors, bins=20, alpha=0.5, label='Errori XGBoost')
plt.xlabel('Errore')
plt.ylabel('Frequenza')
plt.title('Distribuzione degli Errori')
plt.legend()
plt.show()
