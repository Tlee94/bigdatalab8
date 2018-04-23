    # Timothy Lee
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession

def main(sc):
    rdd = sc.parallelize(range(1000), 10)
    print (rdd.mean())

if __name__ == "__main__":
    #reload(sys)
    #sys.setdefaultencoding('utf8')
    sc = SparkContext()
    # Execute Main functionality
    df = SparkSession(sc)
    df = df.read.load('nyc_restaurants.csv',
                      format='csv',
                      header=True,
                      inferSchema=True)

    dfrestaurants = df.groupBy('`CUISINE DESCRIPTION`').count()
    dfrestaurants = dfrestaurants.sort(['count'], ascending=False)
    dfrestaurants.show(85)

