"""
Encargado de orquestar los procesos de generacion del datalake
"""
import luigi
from luigi.mock import MockTarget
import create_data_lake
import ingest_data
import transform_data
import clean_data
import compute_daily_prices
import compute_monthly_prices


class GlobalParams(luigi.Config):
    """_summary_

    Args:
    luigi (_type_): _description_
    """

    count = luigi.IntParameter(default=1)


class CreateDatalake(luigi.Task):
    """_summary_

    Args:
        luigi (_type_): _description_
    """

    def output(self):
        """_summary_

        Args:
            luigi (_type_): _description_
        """
        return MockTarget("CreateDatalake")

    def run(self):
        """_summary_

        Args:
            luigi (_type_): _description_
        """
        out = self.output().open("w")
        out.write("complete")
        out.close()
        create_data_lake.create_data_lake()

    def complete(self):
        """_summary_

        Args:
        luigi (_type_): _description_
        """
        print(
            f"{g.count}: complete() Checking to see if {self.__class__.__name__} has been completed"
        )
        g.count += 1
        return self.output().exists()


class IngestData(luigi.Task):
    """_summary_

    Args:
    luigi (_type_): _description_
    """

    def requires(self):
        """_summary_

        Args:
        luigi (_type_): _description_
        """
        return CreateDatalake()

    def output(self):
        """_summary_

        Args:
        luigi (_type_): _description_
        """
        return MockTarget("IngestData")

    def run(self):
        """_summary_

        Args:
            luigi (_type_): _description_
        """
        ingest_data.ingest_data()
        out = self.output().open("w")
        out.write("complete")
        out.close()
        print(f"{g.count}: All Tasks are completed")

    def complete(self):
        """_summary_

        Args:
            luigi (_type_): _description_
        """
        print(
            f"{g.count}: complete() Checking to see if {self.__class__.__name__} has been completed"
        )
        g.count += 1

        if self.output().exists():
            print(f"{g.count}: All Tasks are completed")
        return self.output().exists()


class TransformData(luigi.Task):
    """_summary_

    Args:
    luigi (_type_): _description_
    """

    def requires(self):
        """_summary_

        Args:
        luigi (_type_): _description_
        """
        return IngestData()

    def output(self):
        """_summary_

        Args:
        luigi (_type_): _description_
        """
        return MockTarget("TransformData")

    def run(self):
        """_summary_

        Args:
        luigi (_type_): _description_
        """
        transform_data.transform_data()
        out = self.output().open("w")
        out.write("complete")
        out.close()
        print(f"{g.count}: All Tasks are completed")

    def complete(self):
        """_summary_

        Args:
            luigi (_type_): _description_
        """
        print(
            f"{g.count}: complete() Checking to see if {self.__class__.__name__} has been completed"
        )
        g.count += 1

        if self.output().exists():
            print(f"{g.count}: All Tasks are completed")
        return self.output().exists()


class CleanData(luigi.Task):
    """_summary_

    Args:
    luigi (_type_): _description_
    """

    def requires(self):
        """_summary_

        Args:
        luigi (_type_): _description_
        """
        return TransformData()

    def output(self):
        """_summary_

        Args:
        luigi (_type_): _description_
        """
        return MockTarget("CleanData")

    def run(self):
        """_summary_

        Args:
        luigi (_type_): _description_
        """
        clean_data.clean_data()
        out = self.output().open("w")
        out.write("complete")
        out.close()
        print(f"{g.count}: All Tasks are completed")

    def complete(self):
        """_summary_

        Args:
        luigi (_type_): _description_
        """
        print(
            f"{g.count}: complete() Checking to see if {self.__class__.__name__} has been completed"
        )
        g.count += 1

        if self.output().exists():
            print(f"{g.count}: All Tasks are completed")
        return self.output().exists()


class ComputeDailyPrices(luigi.Task):
    """_summary_

    Args:
    luigi (_type_): _description_
    """

    def requires(self):
        """_summary_

        Args:
        luigi (_type_): _description_
        """
        return CleanData()

    def output(self):
        """_summary_

        Args:
        luigi (_type_): _description_
        """
        return MockTarget("ComputeDailyPrices")

    def run(self):
        """_summary_

        Args:
            luigi (_type_): _description_
        """
        compute_daily_prices.compute_daily_prices()
        out = self.output().open("w")
        out.write("complete")
        out.close()
        print(f"{g.count}: All Tasks are completed")

    def complete(self):
        """_summary_

        Args:
        luigi (_type_): _description_
        """
        print(
            f"{g.count}: complete() Checking to see if {self.__class__.__name__} has been completed"
        )
        g.count += 1

        if self.output().exists():
            print(f"{g.count}: All Tasks are completed")
        return self.output().exists()


class ComputeMonthlyPrices(luigi.Task):
    """_summary_

    Args:
    luigi (_type_): _description_
    """

    def requires(self):
        """_summary_

        Args:
        luigi (_type_): _description_
        """
        return ComputeDailyPrices()

    def output(self):
        """_summary_

        Args:
        luigi (_type_): _description_
        """
        return MockTarget("ComputeMonthlyPrices")

    def run(self):
        """_summary_

        Args:
        luigi (_type_): _description_
        """
        compute_monthly_prices.compute_monthly_prices()
        out = self.output().open("w")
        out.write("complete")
        out.close()
        print(f"{g.count}: All Tasks are completed")

    def complete(self):
        """_summary_

        Args:
        luigi (_type_): _description_
        """
        print(
            f"{g.count}: complete() Checking to see if {self.__class__.__name__} has been completed"
        )
        g.count += 1

        if self.output().exists():
            print(f"{g.count}: All Tasks are completed")
        return self.output().exists()


g = GlobalParams()
luigi.build([ComputeMonthlyPrices()], local_scheduler=True)
