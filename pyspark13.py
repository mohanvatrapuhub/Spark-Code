#Finding topN priced products by category using sorted and ranked functions in spark using pyspark

def getTopDenseN(rec, topN):
  x = [ ]
  topNPrices = [ ]
  prodPrices = [ ]
  prodPricesDesc = [ ]
  for i in rec[1]:
    prodPrices.append(float(i.split(",")[4]))
  prodPricesDesc = list(sorted(set(prodPrices), reverse=True))
  import itertools
  topNPrices = list(itertools.islice(prodPricesDesc, 0, topN))
  for j in sorted(rec[1], key=lambda k: float(k.split(",")[4]), reverse=True):
    if(float(j.split(",")[4]) in topNPrices):
      x.append(j)
  return (y for y in x)


products = sc.textFile("/user/cloudera/import/products")

productsMap = products.map(lambda rec: (rec.split(",")[1], rec))

for i in productsMap.groupByKey().flatMap(lambda x: getTopDenseN(x, 2)).collect(): print(i)