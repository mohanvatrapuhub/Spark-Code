#Joining Datasets and finding the revenue per day and orders per day in spark using pyspark and sql

from pyspark.sql import SQLContext, Row

sqlContext = SQLContext(sc)
sqlContext.sql("set spark.sql.shuffle.partitions=10");

#Getting the orders dataset and storing the results in a temporary table
ordersRDD = sc.textFile("/user/cloudera/import/orders")
ordersMap = ordersRDD.map(lambda o: o.split(","))
orders = ordersMap.map(lambda o: Row(order_id=int(o[0]), order_date=o[1], \
order_customer_id=int(o[2]), order_status=o[3]))
ordersSchema = sqlContext.inferSchema(orders)
ordersSchema.registerTempTable("orders")

#Getting the order_items dataset and storing the results in temporary table
orderItemsRDD = sc.textFile("/user/cloudera/import/order_items")
orderItemsMap = orderItemsRDD.map(lambda oi: oi.split(","))
orderItems = orderItemsMap.map(lambda oi: Row(order_item_id=int(oi[0]), order_item_order_id=int(oi[1]), \
order_item_product_id=int(oi[2]), order_item_quantity=int(oi[3]), order_item_subtotal=float(oi[4]), \
order_item_product_price=float(oi[5])))
orderItemsSchema = sqlContext.inferSchema(orderItems)
orderItemsSchema.registerTempTable("order_items")

#Joining both the datasets
joinAggData = sqlContext.sql("select o.order_date, sum(oi.order_item_subtotal), \
count(distinct o.order_id) from orders o join order_items oi \
on o.order_id = oi.order_item_order_id \
group by o.order_date order by o.order_date")

for data in joinAggData.collect():
  print(data)
