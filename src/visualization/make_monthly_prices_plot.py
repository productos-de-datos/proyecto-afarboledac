"""
Funciones encargadas de generar graficos con los precios promedio mensuales
dada la informacion en la zona business guardandolo en bussiness/reports/figures
"""
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


def make_monthly_prices_plot():
    """Crea un grafico de lines que representa los precios promedios diarios.

    Usando el archivo data_lake/business/precios-diarios.csv, crea un grafico de
    lines que representa los precios promedios diarios.

    El archivo se debe salvar en formato PNG en data_lake/business
    /reports/figures/monthly_prices.png.
    """
    module_path = os.path.dirname(__file__)

    file_path = os.path.join(
        module_path, "../../data_lake/business/precios-mensuales.csv"
    )

    precios_mensuales = cargar_archivo(file_path)
    fig = graficar_serie_tiempo(
        precios_mensuales, "Precio Mensual Promedio Bolsa Nacional"
    )

    target_folder = os.path.join(
        module_path, "../../data_lake/business/reports/figures/monthly_prices.png"
    )

    fig.savefig(target_folder)


def graficar_serie_tiempo(precios, titulo):
    """
    funcion encargada de graficar una serie de tiempo dado un dataframe
    con los precios de la energia, y el titulo de la grafica
    """
    color_price = "#3399e6"
    color_temperature = "#69b3a2"

    date = precios["Fecha"]
    value = precios["Precio"]

    fig, axis = plt.subplots(figsize=(20, 8))
    fig.suptitle(titulo, fontsize=20)

    axis.set_xlabel("Fecha", color=color_temperature, fontsize=14)
    axis.set_ylabel("Precio ($)", color=color_price, fontsize=14)

    axis.xaxis.set_major_locator(mdates.YearLocator())
    axis.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

    axis.plot(date, value)
    return fig


def cargar_archivo(file_path):
    """
    funcion encargada de cargar en un dataframe dado una ruta de archivo
    """
    precios_diarios = pd.read_csv(file_path)

    precios_diarios["Fecha"] = pd.to_datetime(
        precios_diarios["Fecha"], format="%Y-%m-%d"
    )

    return precios_diarios


if __name__ == "__main__":
    import doctest

    make_monthly_prices_plot()
    doctest.testmod()
