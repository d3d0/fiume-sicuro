import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import pymysql
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score

# Connessione al database
connection = pymysql.connect(
    host='127.0.0.1',
    user='root',
    password='root',
    database='fiumesicuro'
)

# Chiedere all'utente se analizzare tutte le stazioni o una sola
scelta = input("Vuoi analizzare tutte le stazioni (T) o solo una stazione specifica (S)? ").strip().upper()

if scelta == 'S':
    stazione_id = input("Inserisci l'ID della stazione che vuoi analizzare: ")
    query = f"""
    SELECT stazione_id, nome_stazione, data_ora_rilevazione, livello_idro, temperatura, precipitazione_1h, umidita_2m 
    FROM vista_livello_temperatura 
    WHERE stazione_id = {stazione_id}
    """
else:
    query = """
    SELECT stazione_id, nome_stazione, data_ora_rilevazione, livello_idro, temperatura, precipitazione_1h, umidita_2m 
    FROM vista_livello_temperatura
    """

# Creare un DataFrame Pandas
df = pd.read_sql(query, connection)
connection.close()

# Rimozione dei valori nulli
df = df.dropna(subset=['livello_idro', 'temperatura', 'precipitazione_1h'])

# Visualizzazione delle correlazioni tra le variabili
sns.pairplot(df[['livello_idro', 'temperatura', 'precipitazione_1h']])
plt.show()

# Preparazione dei dati per il machine learning
X = df[['temperatura', 'precipitazione_1h']]
y = df['livello_idro']

# Divisione in training e test set
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Definire il modello di Random Forest senza iperparametri ottimizzati
base_model = RandomForestRegressor(random_state=42)
base_model.fit(X_train, y_train)

# Valutazione del modello di base
y_pred_base = base_model.predict(X_test)
mse_base = mean_squared_error(y_test, y_pred_base)
r2_base = r2_score(y_test, y_pred_base)
print(f"Modello base - Mean Squared Error: {mse_base}, R^2 Score: {r2_base}")

# Definire la griglia di parametri per l'ottimizzazione
param_grid = {
    'n_estimators': [50, 100, 150],
    'max_depth': [10, 20, None],
    'min_samples_split': [2, 5, 10],
    'min_samples_leaf': [1, 2, 4]
}

# Ottimizzazione degli iperparametri con Grid Search e Cross-Validation
grid_search = GridSearchCV(estimator=RandomForestRegressor(random_state=42),
                           param_grid=param_grid,
                           cv=5,  # 5-fold cross-validation
                           n_jobs=-1,  # Usa tutti i core
                           scoring='neg_mean_squared_error',  # Minimizza MSE
                           verbose=2)

grid_search.fit(X_train, y_train)

# Migliori parametri e modello ottimizzato
best_params = grid_search.best_params_
print(f"Migliori parametri trovati: {best_params}")

best_model = grid_search.best_estimator_

# Valutazione del modello ottimizzato
y_pred_optimized = best_model.predict(X_test)
mse_optimized = mean_squared_error(y_test, y_pred_optimized)
r2_optimized = r2_score(y_test, y_pred_optimized)
print(f"Modello ottimizzato - Mean Squared Error: {mse_optimized}, R^2 Score: {r2_optimized}")

# Visualizzare previsioni vs valori reali per il modello ottimizzato
plt.figure(figsize=(10, 6))
plt.scatter(y_test, y_pred_optimized, alpha=0.6, color='blue')
plt.plot([y.min(), y.max()], [y.min(), y.max()], 'r--')  # Linea di perfetta previsione
plt.xlabel("Valori Reali di Livello Idrometrico")
plt.ylabel("Valori Predetti di Livello Idrometrico")
plt.title("Predizioni del Modello Ottimizzato vs Valori Reali")
plt.show()
