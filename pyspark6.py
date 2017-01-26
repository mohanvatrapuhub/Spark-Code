#Performing aggregations on the datasets to find the total revenue

ordersRDD = sc.textFile("/user/cloudera/import/orders")
ordersRDD.count()

orderItemsRDD = sc.textFile("/user/cloudera/import/order_items")
orderItemsMap = orderItemsRDD.map(lambda rec: float(rec.split(",")[4]))
for i in orderItemsMap.take(5):
  print i

#Perform aggregation by reduce function - sum
orderItemsReduce = orderItemsMap.reduce(lambda rev1, rev2: rev1 + rev2)