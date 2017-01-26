#To get topN priced products by category
def getTopDenseN(rec: (String, Iterable[String]), topN: Int): Iterable[String] = {
  var prodPrices: List[Float] = List()
  var topNPrices: List[Float] = List()
  var sortedRecs: List[String] = List()
  for(i <- rec._2) {
    prodPrices = prodPrices:+ i.split(",")(4).toFloat
  }
  topNPrices = prodPrices.distinct.sortBy(k => -k).take(topN)
  sortedRecs = rec._2.toList.sortBy(k => -k.split(",")(4).toFloat) 
  var x: List[String] = List()
  for(i <- sortedRecs) {
    if(topNPrices.contains(i.split(",")(4).toFloat))
      x = x:+ i 
  }
  return x
}

val products = sc.textFile("/user/cloudera/import/products")

val productsMap = products.map(rec => (rec.split(",")(1), rec))

productsMap.groupByKey().flatMap(x => getTopDenseN(x, 2)).collect().foreach(println)