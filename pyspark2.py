#Using hive context in spark using pyspark

from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)
depts = sqlContext.sql("select * from departments")
for rec in depts.collect():
  print(rec)

sqlContext.sql("create table departmentsAgain as select * from departments")
depts = sqlContext.sql("select * from departmentsAgain")
for rec in depts.collect():
  print(rec)