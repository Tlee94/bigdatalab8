#Timothy Lee
from pyspark import SparkContext
from pyspark.sql import SparkSession

if __name__ == "__main__":
    sc = SparkContext()
    # Execute Main functionality
    df = SparkSession(sc)
    df = df.read.load('nyc_restaurants.csv',
                     format='csv',
                     header=True,
                     inferSchema=True)


    dfrestaurants = df.groupBy('`CUISINE DESCRIPTION`').count()
    dfrestaurants = dfrestaurants.sort(['count'],ascending = False)
    dfrestaurants.show(85)
