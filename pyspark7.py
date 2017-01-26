#Finding the number of orders by using aggregate functions

ordersRDD = sc.textFile("/user/cloudera/import/orders") #Reading data from HDFS into spark

ordersMapRDD = ordersRDD.map(lambda rec: ((rec.split(",")[1], rec.split(",")[3]), 1)) #Mapping the dataset

ordersByStatusPerDay = ordersMapRDD.reduceByKey(lambda v1, v2: v1+v2) #Aggregation

for i in ordersByStatusPerDay.collect():
  print(i)