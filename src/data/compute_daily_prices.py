"""
Funciones encargadas de computar los precios promedio diarios apartir de los archivos
de la zona cleansed y son guardados en la zona business
"""
import os
import pandas as pd


def compute_daily_prices():
    """Compute los precios promedios diarios.

    Usando el archivo data_lake/cleansed/precios-horarios.csv, compute el prcio
    promedio diario (sobre las 24 horas del dia) para cada uno de los dias. Las
    columnas del archivo data_lake/business/precios-diarios.csv son:

    * fecha: fecha en formato YYYY-MM-DD

    * precio: precio promedio diario de la electricidad en la bolsa nacional
    >>> compute_daily_prices()
           Fecha    Precio
    0 1995-07-20  1.409435
    1 1995-07-21  4.924333
    2 1995-07-22  1.269500
    3 1995-07-23  0.953083
    4 1995-07-24  4.305917
    5 1995-07-25  1.149167
    6 1995-07-26  1.108625
    7 1995-07-27  0.585958
    8 1995-07-28  0.499792
    9 1995-07-29  0.927667
    """
    module_path = os.path.dirname(__file__)
    folder_path = os.path.join(
        module_path, "../../data_lake/cleansed/precios-horarios.csv"
    )
    target_folder = os.path.join(
        module_path, "../../data_lake/business/precios-diarios.csv"
    )

    precios_horarios = pd.read_csv(folder_path)

    precios_horarios["Fecha"] = pd.to_datetime(
        precios_horarios["Fecha"], format="%Y-%m-%d"
    )
    precios_horarios["Precio"] = precios_horarios.iloc[:, 1:].mean(axis=1)

    precios_diarios = precios_horarios[["Fecha", "Precio"]]

    precios_diarios.to_csv(target_folder, index=False)
    print(precios_diarios.head(10))


if __name__ == "__main__":
    import doctest

    compute_daily_prices()
    doctest.testmod()
