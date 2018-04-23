    # Timothy Lee
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession

def restaurants(sc):
    df = SparkSession(sc)
    df = df.read.load('nyc_restaurants.csv',
                      format='csv',
                      header=True,
                      inferSchema=True)

    dfrestaurants = df.groupBy('`CUISINE DESCRIPTION`').count()
    dfrestaurants = dfrestaurants.sort(['count'], ascending=False)
    dfrestaurants.show(85)


if __name__ == "__main__":
    sc = SparkContext()
    # Execute Main functionality
    restaurants(sc)


