#Filtering dataset into small datasets using spark

ordersRDD = sc.textFile("/user/cloudera/import/orders")

for i in ordersRDD.filter(lambda line: line.split(",")[3] == "COMPLETE").take(5): print(i)

for i in ordersRDD.filter(lambda line: "PENDING" in line.split(",")[3]).take(5): print(i)

for i in ordersRDD.filter(lambda line: int(line.split(",")[0]) > 100).take(5): print(i)
 
for i in ordersRDD.filter(lambda line: int(line.split(",")[0]) > 100 or line.split(",")[3] in "PENDING").take(5): print(i)
 
for i in ordersRDD.filter(lambda line: int(line.split(",")[0]) > 1000 and ("PENDING" in line.split(",")[3] or line.split(",")[3] == ("CANCELLED"))).take(5): print(i)
 