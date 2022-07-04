"""
Funciones encargadas de generar graficos con los precios promedio mensuales
dada la informacion en la zona business guardandolo en bussiness/reports/figures
"""
import os
import make_daily_prices_plot


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

    precios_mensuales = make_daily_prices_plot.cargar_archivo(file_path)
    fig = make_daily_prices_plot.graficar_serie_tiempo(
        precios_mensuales, "Precio Mensual Promedio Bolsa Nacional"
    )

    target_folder = os.path.join(
        module_path, "../../data_lake/business/reports/figures/monthly_prices.png"
    )

    fig.savefig(target_folder)


if __name__ == "__main__":
    import doctest

    make_monthly_prices_plot()
    doctest.testmod()
