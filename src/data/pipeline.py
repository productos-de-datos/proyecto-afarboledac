import luigi
from luigi.mock import MockTarget
import create_data_lake
import ingest_data
import transform_data
import clean_data
import compute_daily_prices
import compute_monthly_prices


class GlobalParams(luigi.Config):
    count = luigi.IntParameter(default=1)


class CreateDatalake(luigi.Task):
    def output(self):
        return MockTarget("CreateDatalake")

    def run(self):
        out = self.output().open("w")
        out.write("complete")
        out.close()
        create_data_lake.main()

    def complete(self):
        print(
            f"{g.count}: complete() Checking to see if {self.__class__.__name__} has been completed"
        )
        g.count += 1
        return self.output().exists()


class IngestData(luigi.Task):
    def requires(self):
        return CreateDatalake()

    def output(self):
        return MockTarget("IngestData")

    def run(self):
        ingest_data.main()
        out = self.output().open("w")
        out.write("complete")
        out.close()
        print(f"{g.count}: All Tasks are completed")

    def complete(self):
        print(
            f"{g.count}: complete() Checking to see if {self.__class__.__name__} has been completed"
        )
        g.count += 1

        if self.output().exists():
            print(f"{g.count}: All Tasks are completed")
        return self.output().exists()


class TransformData(luigi.Task):
    def requires(self):
        return IngestData()

    def output(self):
        return MockTarget("TransformData")

    def run(self):
        transform_data.main()
        out = self.output().open("w")
        out.write("complete")
        out.close()
        print(f"{g.count}: All Tasks are completed")

    def complete(self):
        print(
            f"{g.count}: complete() Checking to see if {self.__class__.__name__} has been completed"
        )
        g.count += 1

        if self.output().exists():
            print(f"{g.count}: All Tasks are completed")
        return self.output().exists()


class CleanData(luigi.Task):
    def requires(self):
        return TransformData()

    def output(self):
        return MockTarget("CleanData")

    def run(self):
        clean_data.main()
        out = self.output().open("w")
        out.write("complete")
        out.close()
        print(f"{g.count}: All Tasks are completed")

    def complete(self):
        print(
            f"{g.count}: complete() Checking to see if {self.__class__.__name__} has been completed"
        )
        g.count += 1

        if self.output().exists():
            print(f"{g.count}: All Tasks are completed")
        return self.output().exists()


class ComputeDailyPrices(luigi.Task):
    def requires(self):
        return CleanData()

    def output(self):
        return MockTarget("ComputeDailyPrices")

    def run(self):
        compute_daily_prices.main()
        out = self.output().open("w")
        out.write("complete")
        out.close()
        print(f"{g.count}: All Tasks are completed")

    def complete(self):
        print(
            f"{g.count}: complete() Checking to see if {self.__class__.__name__} has been completed"
        )
        g.count += 1

        if self.output().exists():
            print(f"{g.count}: All Tasks are completed")
        return self.output().exists()


class ComputeMonthlyPrices(luigi.Task):
    def requires(self):
        return ComputeDailyPrices()

    def output(self):
        return MockTarget("ComputeMonthlyPrices")

    def run(self):
        compute_monthly_prices.main()
        out = self.output().open("w")
        out.write("complete")
        out.close()
        print(f"{g.count}: All Tasks are completed")

    def complete(self):
        print(
            f"{g.count}: complete() Checking to see if {self.__class__.__name__} has been completed"
        )
        g.count += 1

        if self.output().exists():
            print(f"{g.count}: All Tasks are completed")
        return self.output().exists()


g = GlobalParams()
luigi.build([ComputeMonthlyPrices()], local_scheduler=True)
