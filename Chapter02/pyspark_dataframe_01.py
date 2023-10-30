import unittest
import os


class MyTestCase(unittest.TestCase):
    def test_something(self):
        self.assertEqual(True, False)  # add assertion here

    @classmethod
    def setUpClass(cls):
        import pandas as pd

        cls.pd_df = pd.read_csv("1800.csv",
                                names=["stationID", "date", "measure_type", "temperature"],
                                usecols=[0, 1, 2, 3]
                                )

    def test_print_head(self, target):
        print(target.head())

    def test_lecture(self):
        # measure_type 이 TMIN
        pd_min_temps = self.pd_df[self.pd_df['measure_type'] == "TMIN"]

        self.test_print_head(pd_min_temps)

        pd_station_temps = pd_min_temps[["stationID", "temperature"]]

        pd_min_temps_by_station = pd_station_temps.groupby(["stationID"]).min("temperature")

        self.test_print_head(pd_min_temps_by_station)


if __name__ == '__main__':
    unittest.main()
