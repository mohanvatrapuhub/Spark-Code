#Finding the average revenue per day using aggregate functions in spark using pyspark

ordersRDD = sc.textFile("/user/cloudera/import/orders")
orderItemsRDD = sc.textFile("/user/cloudera/import/order_items")

ordersParsedRDD = ordersRDD.map(lambda rec: (rec.split(",")[0], rec))
orderItemsParsedRDD = orderItemsRDD.map(lambda rec: (rec.split(",")[1], rec))

ordersJoinOrderItems = orderItemsParsedRDD.join(ordersParsedRDD)
ordersJoinOrderItemsMap = ordersJoinOrderItems.map(lambda t: ((t[1][1].split(",")[1], t[0]), float(t[1][0].split(",")[4])))

revenuePerDayPerOrder = ordersJoinOrderItemsMap.reduceByKey(lambda acc, value: acc + value)
revenuePerDayPerOrderMap = revenuePerDayPerOrder.map(lambda rec: (rec[0][0], rec[1]))

#Performing aggregation by using aggregateByKey function
revenuePerDay = revenuePerDayPerOrderMap.aggregateByKey( \
(0, 0), \
lambda acc, revenue: (acc[0] + revenue, acc[1] + 1), \
lambda total1, total2: (round(total1[0] + total2[0], 2), total1[1] + total2[1]) \
)

for data in revenuePerDay.collect():
  print(data)

avgRevenuePerDay = revenuePerDay.map(lambda x: (x[0], x[1][0]/x[1][1]))