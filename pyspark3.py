#Developing a word count program in spark using pyspark

data = sc.textFile("/user/cloudera/wordcount.txt")
dataFlatMap = data.flatMap(lambda x: x.split(" "))
dataMap = dataFlatMap.map(lambda x: (x, 1))
dataReduceByKey = dataMap.reduceByKey(lambda x,y: x + y)

dataReduceByKey.saveAsTextFile("/user/cloudera/wordcountoutput")

for i in dataReduceByKey.collect():
  print(i)