from pyspark.sql import Row
studentEnrollment = Row("StudentId","FirstName","Lastname")
enrollmentList = ["101,Anuj,Patel","102,Daniel,Tang","103,Mehul,Patel","104,Miti,Patel","105,Purvesh,Dave"]
enrollmentDf = sc.parallelize(enrollmentList).map(lambda trans:trans.split(",")).map(lambda r:studentEnrollment(*r)).toDF()
enrollmentDf.show()
suppliers = spark.sql("select * from miti_db.suppliers LIMIT 7")
suppliers.count().show()
dropduplicatsupplier=suppliers.dropDuplicates()
dropduplicatsupplier.count().show()
quit()
