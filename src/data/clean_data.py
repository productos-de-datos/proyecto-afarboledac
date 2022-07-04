"""
Funciones para limpiarlos datos de la zona landing a la zona raw
"""
import os
import glob
import pandas as pd


def buscar_cabecera(file_path):
    """
    buscar_cabecera para encontrar el header de un archivo, retornando la posicion del header
    """
    with open(file_path, mode="r", encoding="UTF-8") as myfile:
        header_position = 0
        for line in myfile:
            header_position += 1
            if "Fecha" in line:
                return header_position

    return header_position


def cargar_archivo(file_path):
    """
    Carga el archivo en csv y omite la cabecera
    """
    archivo_encontrado = pd.read_csv(
        file_path, skiprows=buscar_cabecera(file_path), header=None, index_col=0,
    )

    return archivo_encontrado


def consolidar_archivos(file_path):
    """
    Funcion encargada de tomar todos los archivos dada una ruta y consolidarlos
    """
    appended_data = []
    for file in file_path:
        archivo_formateado = cargar_archivo(file)
        appended_data.append(archivo_formateado)

    resultado = pd.concat(appended_data, ignore_index=True).reset_index(drop=True)

    return resultado


def formatear_archivo(resultado):
    """
    Para un archivo le asigna la cabecera general utilizada para consolidar
    """
    cabecera_subset = [
        "Fecha",
        "0",
        "1",
        "2",
        "3",
        "4",
        "5",
        "6",
        "7",
        "8",
        "9",
        "10",
        "11",
        "12",
        "13",
        "14",
        "15",
        "16",
        "17",
        "18",
        "19",
        "20",
        "21",
        "22",
        "23",
        "Version",
        "X",
    ]
    resultado.columns = cabecera_subset
    resultado = resultado.drop(columns=["Version", "X"])

    resultado["Fecha"] = pd.to_datetime(resultado["Fecha"], format="%Y-%m-%d")
    resultado = resultado.dropna(how="all")
    resultado = resultado.sort_values(by="Fecha", ascending=True)

    return resultado


def clean_data():
    """Realice la limpieza y transformación de los archivos CSV.

    Usando los archivos data_lake/raw/*.csv, cree el archivo
    data_lake/cleansed/precios-horarios.csv.
    Las columnas de este archivo son:

    * fecha: fecha en formato YYYY-MM-DD
    * hora: hora en formato HH
    * precio: precio de la electricidad en la bolsa nacional
    Este archivo contiene toda la información del 1997 a 2021.
    >>> clean_data()
           Fecha      0      1      2      3      4      5      6      7      8      9     10     11     12     13     14     15     16     17      18      19      20     21     22     23
    0 1995-07-20    NaN  1.073  1.073  1.073  1.073  1.073  1.073  1.073  1.074  1.074  2.827  2.827  2.827  1.074  1.073  1.073  1.073  1.073   1.074   1.897   1.897  1.897  1.073  1.073
    1 1995-07-21  1.073  1.000  1.000  1.000  1.000  1.000  5.000  6.000  6.000  6.000  6.000  9.256  9.256  5.000  5.000  1.000  1.000  5.000  12.000  16.670  11.929  5.000  1.000  1.000
    2 1995-07-22  1.073  1.073  1.000  1.000  1.000  1.073  1.303  1.303  1.303  1.303  1.303  1.303  1.303  1.303  1.073  1.000  1.000  1.000   1.303   2.500   2.500  1.303  1.073  1.073
    3 1995-07-23  1.073  1.000  1.000  1.000  1.000  1.000  0.100  1.000  1.000  1.000  1.000  1.000  1.238  1.238  1.000  0.100  0.100  1.000   1.238   1.238   1.238  1.238  1.073  1.000
    4 1995-07-24  1.000  1.000  0.990  1.000  1.000  1.073  3.000  3.000  3.000  3.500  8.845  9.256  3.000  1.073  1.073  1.073  3.000  2.000  18.630  22.500   9.256  3.000  1.073  1.000
    """
    module_path = os.path.dirname(__file__)
    folder_path = os.path.join(module_path, "../../data_lake/raw/*")
    target_path = os.path.join(
        module_path, "../../data_lake/cleansed/precios-horarios.csv"
    )
    files = glob.glob(folder_path)

    resultado = consolidar_archivos(files)
    resultado = formatear_archivo(resultado)
    resultado = resultado.drop_duplicates()
    print(resultado.head())
    resultado.to_csv(target_path, index=False)


if __name__ == "__main__":
    import doctest

    clean_data()
    doctest.testmod()
