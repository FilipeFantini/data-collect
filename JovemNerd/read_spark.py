#%%
from pyspark.sql import SparkSession

spark = (SparkSession .builder 
                      .appName("Python Spark SQL basic example") 
                      .config("spark.some.config.option", "some-value") 
                      .getOrCreate())

#%%

df = spark.read.json.load("data\episodios\json")

df.show()