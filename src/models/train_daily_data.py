import pandas as pd
from sklearn.metrics import mean_squared_error
import matplotlib.pyplot as plt
import numpy as np
import os
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from sklearn.neural_network import MLPRegressor
import pickle


def main():
    train_daily_model()


def train_daily_model():
    """Entrena el modelo de pronóstico de precios diarios.

    Con las features entrene el modelo de proóstico de precios diarios y
    salvelo en models/precios-diarios.pkl


    """

    module_path = os.path.dirname(__file__)

    precios_diarios = cargar_archivo(module_path)

    target_folder = os.path.join(module_path, "../../models/precios-diarios.pkl")
    model_path = os.path.join(module_path, "precios-diarios.pkl")

    steps = 1800
    P = 30

    fechas = precios_diarios.Fecha.values
    precios = precios_diarios.Precio.values

    precios_scaled = escalar_precios(precios)
    X = []
    for t in range(P - 1, len(precios_scaled) - 1):
        X.append([precios_scaled[t - n] for n in range(P)])

    observed_scaled = precios_scaled[P:]

    np.random.seed(123456)

    H = 1  # Se escoge arbitrariamente

    mlp = MLPRegressor(
        hidden_layer_sizes=(H,),
        activation="logistic",
        learning_rate="adaptive",
        momentum=0.0,
        learning_rate_init=0.1,
        max_iter=10000,
    )

    # Entrenamiento
    mlp.fit(X[0 : len(precios) - steps], observed_scaled[0 : len(precios) - steps])

    with open(model_path, "wb") as files:
        pickle.dump(mlp, files)

    with open(target_folder, "wb") as files:
        pickle.dump(mlp, files)

    # Pronostico
    # y_scaled_m1 = mlp.predict(X)
    # fechas_scaled = fechas[P:]


def cargar_archivo(module_path):
    folder_path = os.path.join(
        module_path, "../../data_lake/business/precios-diarios.csv"
    )
    precios_diarios = pd.read_csv(folder_path, parse_dates=["Fecha"])
    return precios_diarios


def escalar_precios(precios):
    #
    # Como primer paso se escala la serie al intervalo [0, 1]
    # ya que esto facilita el entrenamiento del modelo
    #

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

    main()
    doctest.testmod()
