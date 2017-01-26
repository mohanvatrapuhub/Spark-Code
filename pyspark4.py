#Joining datasets in spark using pyspark and finding the revenue and number of orders per day

#Reading data from HDFS into spark
ordersRDD = sc.textFile("/user/cloudera/import/orders")
orderItemsRDD = sc.textFile("/user/cloudera/import/order_items")

#Performing transformation to map the data
ordersParsedRDD = ordersRDD.map(lambda rec: (int(rec.split(",")[0]), rec))
orderItemsParsedRDD = orderItemsRDD.map(lambda rec: (int(rec.split(",")[1]), rec))

#Joining two datasets
ordersJoinOrderItems = orderItemsParsedRDD.join(ordersParsedRDD)

#Finding the number of orders per day
ordersPerDay = ordersJoinOrderItems.map(lambda rec: rec[1][1].split(",")[1] + "," + str(rec[0])).distinct()
ordersPerDayParsedRDD = ordersPerDay.map(lambda rec: (rec.split(",")[0], 1))
totalOrdersPerDay = ordersPerDayParsedRDD.reduceByKey(lambda x, y: x + y)

#Finding the revenue per day
revenuePerOrderPerDay = ordersJoinOrderItems.map(lambda t: (t[1][1].split(",")[1], float(t[1][0].split(",")[4])))
totalRevenuePerDay = revenuePerOrderPerDay.reduceByKey( \
lambda total1, total2: total1 + total2 \
)
for data in totalRevenuePerDay.collect():
  print(data)
  
#Joining orders per day and revenue per day
finalJoinRDD = totalOrdersPerDay.join(totalRevenuePerDay)
for data in finalJoinRDD.take(5):
  print(data)