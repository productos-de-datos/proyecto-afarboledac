import os
import pandas as pd


def main():
    make_features()


def make_features():
    """Prepara datos para pronóstico.

    Cree el archivo data_lake/business/features/precios-diarios.csv. Este
    archivo contiene la información para pronosticar los precios diarios de la
    electricidad con base en los precios de los días pasados. Las columnas
    correspoden a las variables explicativas del modelo, y debe incluir,
    adicionalmente, la fecha del precio que se desea pronosticar y el precio
    que se desea pronosticar (variable dependiente).

    En la carpeta notebooks/ cree los notebooks de jupyter necesarios para
    analizar y determinar las variables explicativas del modelo.

    """
    module_path = os.path.dirname(__file__)

    folder_path = os.path.join(
        module_path, "../../data_lake/business/precios-diarios.csv"
    )
    target_folder = os.path.join(
        module_path, "../../data_lake/business/features/precios_diarios.csv"
    )

    min_mensual, max_mensual = calcular_features(folder_path)
    guardar_features(target_folder, min_mensual, max_mensual)


def guardar_features(target_folder, min_mensual, max_mensual):
    features = pd.concat([min_mensual, max_mensual], axis=1)
    features.to_csv(target_folder, index=True)


def calcular_features(folder_path):
    precios_diarios = pd.read_csv(folder_path, parse_dates=["Fecha"])
    min_mensual = (
        precios_diarios.groupby(pd.Grouper(key="Fecha", freq="M"))
        .min()
        .rename(columns={"Precio": "Precio Minimo Mes"})
    )
    max_mensual = (
        precios_diarios.groupby(pd.Grouper(key="Fecha", freq="M"))
        .max()
        .rename(columns={"Precio": "Precio Maximo Mes"})
    )
    return min_mensual, max_mensual


if __name__ == "__main__":
    import doctest

    main()
    doctest.testmod()
