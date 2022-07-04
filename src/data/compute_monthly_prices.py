"""
Funciones encargadas de computar los precios promedio diarios apartir de los archivos
de la zona cleansed y son guardados en la zona business
"""

import os
import pandas as pd


def compute_monthly_prices():
    """Compute los precios promedios mensuales.

    Usando el archivo data_lake/cleansed/precios-horarios.csv, compute el prcio
    promedio mensual. Las
    columnas del archivo data_lake/business/precios-mensuales.csv son:

    * fecha: fecha en formato YYYY-MM-DD

    * precio: precio promedio mensual de la electricidad en la bolsa nacional
    >>> compute_monthly_prices()
                   Precio
    Fecha                
    1995-07-31   1.540199
    1995-08-31   7.086462
    1995-09-30  10.955819
    1995-10-31  10.445442
    1995-11-30  27.534782
    """

    module_path = os.path.dirname(__file__)
    folder_path = os.path.join(
        module_path, "../../data_lake/cleansed/precios-horarios.csv"
    )
    target_folder = os.path.join(
        module_path, "../../data_lake/business/precios-mensuales.csv"
    )

    precios_horarios = pd.read_csv(folder_path)
    precios_horarios["Fecha"] = pd.to_datetime(
        precios_horarios["Fecha"], format="%Y-%m-%d"
    )

    precios_horarios["Precio"] = precios_horarios.iloc[:, 1:].mean(axis=1)
    precios_horarios = precios_horarios[["Fecha", "Precio"]]

    precios_mensual = precios_horarios.groupby(pd.Grouper(key="Fecha", freq="M")).mean()
    precios_mensual.to_csv(target_folder, index=True)
    print(precios_mensual.head(5))


if __name__ == "__main__":
    import doctest

    compute_monthly_prices()
    doctest.testmod()
