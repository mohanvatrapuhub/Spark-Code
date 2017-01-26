#Finding topN products by price in each category using sorted functions where data is sorted and ranked in spark 

def getTopN(rec, topN):
  x = [ ]
  x = list(sorted(rec[1], key=lambda k: float(k.split(",")[4]), reverse=True))
  import itertools
  return (y for y in list(itertools.islice(x, 0, topN)))

products = sc.textFile("/user/cloudera/import/products")

productsMap = products.map(lambda rec: (rec.split(",")[1], rec))

for i in productsMap.groupByKey().flatMap(lambda x: getTopN(x, 2)).collect(): print(i)
