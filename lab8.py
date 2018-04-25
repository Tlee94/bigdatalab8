import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession


def group_restaurants(sc):
    # Create a SparkSession object with sc SparkContext as its argument
    spark = SparkSession(sc)

    # Load the csv file with the appropriate configurations
    rest_df = spark.read.csv('nyc_restaurants.csv', header=True, inferSchema=True).cache()

    # Perform aggregate operation by `CUISINE_DESCRIPTION` column and sort the result
    rest_df = rest_df.groupBy('CUISINE DESCRIPTION').count()
    rest_df = rest_df.sort('count', ascending=False)

    # Show all categories of restaurants and associated number of restaurants
    rest_df.show(n=rest_df.count(), truncate=False)


if __name__ == '__main__':
    # Set the encoding to UTF-8
    reload(sys)
    sys.setdefaultencoding('utf8')

    # Create a SparkContext object and execute the main() function
    sc = SparkContext()
    group_restaurants(sc)