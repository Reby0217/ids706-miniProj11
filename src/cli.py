from pyspark.sql import SparkSession
from pyspark.sql.functions import avg


def main():
    # Initialize a Spark session
    spark = SparkSession.builder.appName(
        "Top1000WealthiestDataProcessing"
    ).getOrCreate()

    # Load the CSV file into a DataFrame
    df = spark.read.csv(
        "src/Top_1000_wealthiest_people.csv", header=True, inferSchema=True
    )

    # Data transformation: Calculate the average net worth per industry
    industry_avg_net_worth = df.groupBy("Industry").agg(
        avg("Net Worth (in billions)").alias("Average_Net_Worth")
    )

    # Spark SQL Query: Register the DataFrame as a SQL temporary view and query the top 5 countries by average net worth
    df.createOrReplaceTempView("wealth_data")
    top_countries_by_net_worth = spark.sql(
        """
        SELECT Country, AVG(`Net Worth (in billions)`) AS Average_Net_Worth
        FROM wealth_data
        GROUP BY Country
        ORDER BY Average_Net_Worth DESC
        LIMIT 5
    """
    )

    # Show results
    industry_avg_net_worth.show()
    top_countries_by_net_worth.show()

    # Return the results for testing purposes
    return industry_avg_net_worth, top_countries_by_net_worth


def generate_report(industry_avg_net_worth, top_countries_by_net_worth):
    # Generate Markdown summary report
    report_content = """# Wealth Data Summary Report

## Average Net Worth by Industry
| Industry | Average Net Worth (in billions) |
|----------|--------------------------------|
"""
    for row in industry_avg_net_worth.collect():
        report_content += f"| {row['Industry']} | {row['Average_Net_Worth']:.2f} |\n"

    report_content += "\n## Top 5 Countries by Average Net Worth\n"
    report_content += "| Country | Average Net Worth (in billions) |\n"
    report_content += "|---------|--------------------------------|\n"
    for row in top_countries_by_net_worth.collect():
        report_content += f"| {row['Country']} | {row['Average_Net_Worth']:.2f} |\n"

    # Save the report to a markdown file
    with open("WealthData_Summary_Report.md", "w") as f:
        f.write(report_content)


if __name__ == "__main__":
    spark = SparkSession.builder.appName(
        "Top1000WealthiestDataProcessing"
    ).getOrCreate()
    industry_avg_net_worth, top_countries_by_net_worth = main()
    generate_report(industry_avg_net_worth, top_countries_by_net_worth)
    spark.stop()
