import os


def main():
    create_data_lake()


def create_data_lake():
    """Cree el data lake con sus capas.

    Esta función debe crear la carpeta `data_lake` en la raiz del proyecto. El data lake contiene
    las siguientes subcarpetas:

    ```
    .
    |
    \___ data_lake/
         |___ landing/
         |___ raw/
         |___ cleansed/
         \___ business/
              |___ reports/
              |    |___ figures/
              |___ features/
              |___ forecasts/

    ```


    """

    module_path = os.path.dirname(__file__)
    folder_estructure = [
        "../../data_lake",
        "../../data_lake/landing",
        "../../data_lake/raw",
        "../../data_lake/cleansed",
        "../../data_lake/business",
        "../../data_lake/business/reports",
        "../../data_lake/business/reports/figures",
        "../../data_lake/business/features",
        "../../data_lake/business/forecasts",
        "../../models",
    ]

    for folder in folder_estructure:
        folder_path = os.path.join(module_path, folder)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
    # print("Data_Lake Creado!")


if __name__ == "__main__":
    import doctest

    main()
    doctest.testmod()
