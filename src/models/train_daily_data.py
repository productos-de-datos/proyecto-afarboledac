"""
Funciones encargadas de entrenar un modelo con los datos de business/precios-diarios.csv
donde se escalan los precios en un rango de -1,1 .Se entrena un modelo MLPRegressor
el cual se guarda en la ruta src/models
"""
import os
import pickle
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from sklearn.neural_network import MLPRegressor


def train_daily_model():
    """Entrena el modelo de pronóstico de precios diarios.

    Con las features entrene el modelo de proóstico de precios diarios y
    salvelo en models/precios-diarios.pkl


    """

    module_path = os.path.dirname(__file__)

    precios_diarios = cargar_archivo(module_path)

    target_folder = os.path.join(module_path, "../../models/precios-diarios.pkl")
    model_path = os.path.join(module_path, "precios-diarios.pkl")

    training_rows = 1800
    dias_pasados = 30

    precios = precios_diarios.Precio.values

    precios_scaled = escalar_precios(precios)
    data = []
    for registro in range(dias_pasados - 1, len(precios_scaled) - 1):
        data.append([precios_scaled[registro - n] for n in range(dias_pasados)])

    observed_scaled = precios_scaled[dias_pasados:]

    np.random.seed(123456)

    hidden_layer_size = 1  # Se escoge arbitrariamente

    mlp = MLPRegressor(
        hidden_layer_sizes=(hidden_layer_size,),
        activation="logistic",
        learning_rate="adaptive",
        momentum=0.0,
        learning_rate_init=0.1,
        max_iter=10000,
    )

    # Entrenamiento
    mlp.fit(
        data[0 : len(precios) - training_rows],
        observed_scaled[0 : len(precios) - training_rows],
    )

    with open(model_path, "wb") as files:
        pickle.dump(mlp, files)

    with open(target_folder, "wb") as files:
        pickle.dump(mlp, files)

    # Pronostico
    # y_scaled_m1 = mlp.predict(X)
    # fechas_scaled = fechas[P:]


def cargar_archivo(module_path):
    """
    Funcion encargada de cargar un archivo en un pandas dada una ruta del archivo
    """
    folder_path = os.path.join(
        module_path, "../../data_lake/business/precios-diarios.csv"
    )
    precios_diarios = pd.read_csv(folder_path, parse_dates=["Fecha"])
    return precios_diarios


def escalar_precios(precios):
    """
    Funcion encargada de escalar los archviso en un rango de -1,1
    """

    # crea el transformador
    scaler = MinMaxScaler()

    # escala la serie
    precios_scaled = scaler.fit_transform(np.array(precios).reshape(-1, 1))

    # z es un array de listas como efecto
    # del escalamiento
    precios_scaled = [u[0] for u in precios_scaled]
    return precios_scaled


if __name__ == "__main__":
    import doctest

    train_daily_model()
    doctest.testmod()
