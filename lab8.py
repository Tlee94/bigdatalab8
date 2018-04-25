import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession



if __name__ == '__main__':
    # Set the encoding to UTF-8
    #reload(sys)
    #sys.setdefaultencoding('utf8')

    # Create a SparkContext object and execute the main() function
    sc = SparkContext()
    spark = SparkSession(sc)
    dfrestaurants = spark.read.csv('nyc_restaurants.csv',
                      header=True,
                      inferSchema=True)

    dfrestaurants = dfrestaurants.groupBy('`CUISINE DESCRIPTION`').count()
    dfrestaurants = dfrestaurants.sort('count', ascending=False)
    dfrestaurants.show(85)