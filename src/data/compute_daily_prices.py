import os
import pandas as pd


def main():
    compute_daily_prices()


def compute_daily_prices():
    """Compute los precios promedios diarios.

    Usando el archivo data_lake/cleansed/precios-horarios.csv, compute el prcio
    promedio diario (sobre las 24 horas del dia) para cada uno de los dias. Las
    columnas del archivo data_lake/business/precios-diarios.csv son:

    * fecha: fecha en formato YYYY-MM-DD

    * precio: precio promedio diario de la electricidad en la bolsa nacional



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


if __name__ == "__main__":
    import doctest

    main()
    doctest.testmod()
