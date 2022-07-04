"""
Módulo de ingestión de datos.
-------------------------------------------------------------------------------

"""
import pandas as pd
import openpyxl
import xlwt
import os


def main():
    ingest_data()


def ingest_data():
    """Ingeste los datos externos a la capa landing del data lake.

    Del repositorio jdvelasq/datalabs/precio_bolsa_nacional/xls/ descarge los
    archivos de precios de bolsa nacional en formato xls a la capa landing. La
    descarga debe realizarse usando únicamente funciones de Python.

    """
    URL = "https://raw.githubusercontent.com/jdvelasq/datalabs/master/datasets/precio_bolsa_nacional/xls/{}"
    files = [
        "1995.xlsx",
        "1996.xlsx",
        "1997.xlsx",
        "1998.xlsx",
        "1999.xlsx",
        "2000.xlsx",
        "2001.xlsx",
        "2002.xlsx",
        "2003.xlsx",
        "2004.xlsx",
        "2005.xlsx",
        "2006.xlsx",
        "2007.xlsx",
        "2008.xlsx",
        "2009.xlsx",
        "2010.xlsx",
        "2011.xlsx",
        "2012.xlsx",
        "2013.xlsx",
        "2014.xlsx",
        "2015.xlsx",
        "2016.xls",
        "2017.xls",
        "2018.xlsx",
        "2019.xlsx",
        "2020.xlsx",
        "2021.xlsx",
    ]

    download_folder = "../../data_lake/landing/"
    module_path = os.path.dirname(__file__)

    for file in files:
        url_file_download = URL.format(file)
        folder_path = os.path.join(module_path, download_folder, file)
        descarga = pd.read_excel(url_file_download)
        descarga.to_excel(folder_path)
    # print("")
    # print("archivos descargados correctamente!!")


if __name__ == "__main__":
    import doctest

    main()
    doctest.testmod()
