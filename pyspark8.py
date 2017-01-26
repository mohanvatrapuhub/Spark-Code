#Finding total revenue by using aggregate functions in spark using pyspark

ordersRDD = sc.textFile("/user/cloudera/import/orders")
orderItemsRDD = sc.textFile("/user/cloudera/import/order_items")

ordersParsedRDD = ordersRDD.map(lambda rec: (rec.split(",")[0], rec))
orderItemsParsedRDD = orderItemsRDD.map(lambda rec: (rec.split(",")[1], rec))

ordersJoinOrderItems = orderItemsParsedRDD.join(ordersParsedRDD)
ordersJoinOrderItemsMap = ordersJoinOrderItems.map(lambda t: (t[1][1].split(",")[1], float(t[1][0].split(",")[4])))

revenuePerDay = ordersJoinOrderItemsMap.reduceByKey(lambda acc, value: acc + value)
for i in revenuePerDay.collect(): print(i)