import os
import pandas as pd
import pickle
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import matplotlib.pyplot as plt


def main():
    make_forecasts()


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

    P = 30
    steps = 1800
    X = []
    for t in range(P - 1, len(precios_scaled) - 1):
        X.append([precios_scaled[t - n] for n in range(P)])

    mlpregresor = cargar_modelo(ruta_modelo)
    y_scaled_m1 = mlpregresor.predict(X)
    fechas_scaled = fechas[P:]

    y_m1 = scaler.inverse_transform([[u] for u in y_scaled_m1])
    y_m1 = [u[0] for u in y_m1]

    guardar_pronostico(forecast_path, fechas, precios, P, y_m1)

    graficar_pronostico(
        forecast_figure_path,
        report_figure_path,
        fechas,
        precios,
        steps,
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
    plt.figure(figsize=(14, 5))
    plt.plot(fechas, precios, ".-k")
    plt.grid()
    plt.plot(fechas_scaled, y_m1, "-r")
    plt.axvline(fechas[len(precios) - steps], color="b", ls="--")
    plt.savefig(forecast_figure_path)
    plt.savefig(report_figure_path)


def guardar_pronostico(target_path, fechas, precios, P, y_m1):
    pronosticos = pd.DataFrame(
        {"Fecha": fechas, "Precios": precios, "Pronostico": [None] * P + y_m1}
    )
    pronosticos.to_csv(target_path, index=False)


def cargar_archivo(module_path):
    folder_path = os.path.join(
        module_path, "../../data_lake/business/precios-diarios.csv"
    )
    precios_diarios = pd.read_csv(folder_path, parse_dates=["Fecha"])
    fechas = precios_diarios.Fecha.values
    precios = precios_diarios.Precio.values
    return fechas, precios


def escalar_precios(precios):
    scaler = MinMaxScaler()

    # escala la serie
    precios_scaled = scaler.fit_transform(np.array(precios).reshape(-1, 1))

    # z es un array de listas como efecto
    # del escalamiento
    precios_scaled = [u[0] for u in precios_scaled]
    return scaler, precios_scaled


def cargar_modelo(ruta):
    with open(ruta, "rb") as f:
        return pickle.load(f)


if __name__ == "__main__":
    import doctest

    main()
    doctest.testmod()
