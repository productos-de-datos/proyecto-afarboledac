"""
Funciones encargadas de hacer un pronostico de los precios de la energia
para lo cual carga un modelo existente en un archivo .pickle en la ruta de src/models
"""
import os
import pickle
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import matplotlib.pyplot as plt


def make_forecasts():
    """Construya los pronosticos con el modelo entrenado final.

    Cree el archivo data_lake/business/forecasts/precios-diarios.csv. Este
    archivo contiene tres columnas:

    * La fecha.

    * El precio promedio real de la electricidad.

    * El pronóstico del precio promedio real.


    """
    module_path = os.path.dirname(__file__)
    ruta_modelo = os.path.join(module_path, "../../models/precios-diarios.pkl")
    forecast_path = os.path.join(
        module_path, "../../data_lake/business/forecasts/precios-diarios.csv"
    )

    forecast_figure_path = os.path.join(
        module_path, "../../data_lake/business/forecasts/pronostico-daily.png"
    )

    report_figure_path = os.path.join(
        module_path, "../../data_lake/business/reports/figures/pronostico-daily.png"
    )

    fechas, precios = cargar_archivo(module_path)
    scaler, precios_scaled = escalar_precios(precios)

    dias_pasados = 30
    training_rows = 1800
    data = []
    for registro in range(dias_pasados - 1, len(precios_scaled) - 1):
        data.append([precios_scaled[registro - n] for n in range(dias_pasados)])

    mlpregresor = cargar_modelo(ruta_modelo)
    y_scaled_m1 = mlpregresor.predict(data)
    fechas_scaled = fechas[dias_pasados:]

    y_m1 = scaler.inverse_transform([[u] for u in y_scaled_m1])
    y_m1 = [u[0] for u in y_m1]

    guardar_pronostico(forecast_path, fechas, precios, dias_pasados, y_m1)

    graficar_pronostico(
        forecast_figure_path,
        report_figure_path,
        fechas,
        precios,
        training_rows,
        fechas_scaled,
        y_m1,
    )


def graficar_pronostico(
    forecast_figure_path,
    report_figure_path,
    fechas,
    precios,
    steps,
    fechas_scaled,
    y_m1,
):
    """
    Funcion encargada de graficar el pronostico
    """
    plt.figure(figsize=(14, 5))
    plt.plot(fechas, precios, ".-k")
    plt.grid()
    plt.plot(fechas_scaled, y_m1, "-r")
    plt.axvline(fechas[len(precios) - steps], color="b", ls="--")
    plt.savefig(forecast_figure_path)
    plt.savefig(report_figure_path)


def guardar_pronostico(target_path, fechas, precios, dias_pasados, y_m1):
    """
    Funcion encargada de guardar el los datos reales y pronostico en un dataframe
    dada una ruta donde va a quedar guardada
    """
    pronosticos = pd.DataFrame(
        {
            "Fecha": fechas,
            "Precios": precios,
            "Pronostico": [None] * dias_pasados + y_m1,
        }
    )
    pronosticos.to_csv(target_path, index=False)


def cargar_archivo(module_path):
    """
    Funcion encargada de cargar un dataframe dada la ubicaci[on del archivo
    """
    folder_path = os.path.join(
        module_path, "../../data_lake/business/precios-diarios.csv"
    )
    precios_diarios = pd.read_csv(folder_path, parse_dates=["Fecha"])
    fechas = precios_diarios.Fecha.values
    precios = precios_diarios.Precio.values
    return fechas, precios


def escalar_precios(precios):
    """
    Funcion encargada de escalar los archviso en un rango de -1,1
    """
    scaler = MinMaxScaler()

    # escala la serie
    precios_scaled = scaler.fit_transform(np.array(precios).reshape(-1, 1))

    # z es un array de listas como efecto
    # del escalamiento
    precios_scaled = [u[0] for u in precios_scaled]
    return scaler, precios_scaled


def cargar_modelo(ruta):
    """
    Funcion encargada de cargar un modelo .pkl dada una ruta
    """
    with open(ruta, "rb") as file:
        return pickle.load(file)


if __name__ == "__main__":
    import doctest

    make_forecasts()
    doctest.testmod()
