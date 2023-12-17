from pyspark import SparkContext, SparkConf
import numpy as np

conf = SparkConf().setAppName("Using NumPy in PySpark")
sc = SparkContext(conf=conf)

# Create a PySpark RDD
rdd = sc.parallelize([1, 2, 3, 4, 5])

# Convert the RDD to a NumPy array
np_array = np.array(rdd.collect())

# Perform operations using NumPy
result = np.sqrt(np_array)

# Convert the result back to a PySpark RDD
result_rdd = sc.parallelize(result)


