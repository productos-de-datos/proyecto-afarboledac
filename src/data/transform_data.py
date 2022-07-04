import glob
import pandas as pd
import os
import subprocess
import sys


def main():
    transform_data()


def transform_data():
    """Transforme los archivos xls a csv.

    Transforme los archivos data_lake/landing/*.xls a data_lake/raw/*.csv. Hay
    un archivo CSV por cada archivo XLS en la capa landing. Cada archivo CSV
    tiene como columnas la fecha en formato YYYY-MM-DD y las horas H00, ...,
    H23.

    """
    subprocess.check_call([sys.executable, "-m", "pip", "install", "openpyxl"])
    subprocess.check_call([sys.executable, "-m", "pip", "install", "xlrd"])

    module_path = os.path.dirname(os.path.realpath(__file__))

    folder_path = os.path.join(module_path, "../../data_lake/landing/*")

    files = glob.glob(folder_path)

    target_folder = os.path.join(module_path, "../../data_lake/raw/")

    for file in files:
        # print("Archivos de landing: " + file)
        xlsx_file = pd.read_excel(file)
        # xlsx_file = xlsx_file.fillna(0)
        filename_absolute = os.path.basename(file)
        file_name = os.path.splitext(filename_absolute)[0]

        filename = os.path.join(target_folder, file_name + ".csv")
        # print("Archivos de raw: " + filename)

        xlsx_file.to_csv(filename)

    # print("Archivos movidos a raw correctamente")


if __name__ == "__main__":
    import doctest

    main()
    doctest.testmod()
