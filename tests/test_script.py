import unittest
from pyspark.sql import SparkSession


class TestWealthDataProcessing(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Initialize Spark session for testing
        cls.spark = SparkSession.builder.appName(
            "TestWealthDataProcessing"
        ).getOrCreate()

        # Load test data
        data = [
            ("Rob Walton", "Mexico", "Finance", 8.5, "Walmart"),
            ("Sergey Brin", "USA", "Automotive", 44.76, "Google"),
            ("Steve Ballmer", "USA", "Manufacturing", 13.43, "Koch Industries"),
            ("Alice Walton", "USA", "Technology", 192.96, "Oracle"),
            ("Mukesh Ambani", "France", "Cosmetics", 17.46, "Microsoft"),
            ("Jeff Bezos", "USA", "Cosmetics", 117.13, "Grupo Carso"),
            ("Amancio Ortega", "USA", "Petrochemicals", 47.87, "Google"),
        ]
        columns = ["Name", "Country", "Industry", "Net Worth (in billions)", "Company"]

        # Create DataFrame
        cls.df = cls.spark.createDataFrame(data, columns)
        cls.df.createOrReplaceTempView("wealth_data")

    def test_industry_avg_net_worth(self):
        # Calculate average net worth by industry using transformation
        industry_avg = (
            self.df.groupBy("Industry").avg("Net Worth (in billions)").collect()
        )
        expected_values = {
            "Finance": 8.5,
            "Automotive": 44.76,
            "Manufacturing": 13.43,
            "Technology": 192.96,
            "Cosmetics": 67.3,  # Average of values in test data
            "Petrochemicals": 47.87,
        }
        for row in industry_avg:
            industry = row["Industry"]
            avg_net_worth = round(row["avg(Net Worth (in billions))"], 2)
            self.assertAlmostEqual(avg_net_worth, expected_values[industry])

    def test_top_countries_by_net_worth(self):
        # Query for top countries by average net worth using SQL
        result = self.spark.sql(
            """
            SELECT Country, AVG(`Net Worth (in billions)`) AS Average_Net_Worth
            FROM wealth_data
            GROUP BY Country
            ORDER BY Average_Net_Worth DESC
            LIMIT 3
        """
        ).collect()

        expected_values = {
            "USA": (44.76 + 13.43 + 192.96 + 117.13 + 47.87) / 5,
            "Mexico": 8.5,
            "France": 17.46,
        }
        for row in result:
            country = row["Country"]
            avg_net_worth = round(row["Average_Net_Worth"], 2)
            self.assertAlmostEqual(avg_net_worth, expected_values[country])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


if __name__ == "__main__":
    unittest.main()
