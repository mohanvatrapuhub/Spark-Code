#Load data from HDFS & storing results back to HDFS using Spark

from pyspark import SparkContext

dataRDD = sc.textFile("/user/cloudera/import/departments")
for line in dataRDD.collect():
    print(line)

print(dataRDD.count())

dataRDD.saveAsTextFile("/user/cloudera/pyspark/departments")