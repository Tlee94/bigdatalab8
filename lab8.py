import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession



if __name__ == '__main__':
    # Set the encoding to UTF-8
    reload(sys)
    sys.setdefaultencoding('utf8')

    # Create a SparkContext object and execute the main() function
    sc = SparkContext()
    df = SparkSession(sc)
    df = df.read.csv('nyc_restaurants.csv',
                      format='csv',
                      header=True,
                      inferSchema=True)

    dfrestaurants = df.groupBy('`CUISINE DESCRIPTION`').count()
    dfrestaurants = dfrestaurants.sort(['count'], ascending=False)
    dfrestaurants.show(85)