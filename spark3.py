from pyspark.sql import Row
studentEnrollment = Row("StudentId","FirstName","Lastname")
enrollmentList = ["101,Anuj,Patel","102,Daniel,Tang","103,Mehul,Patel","104,Miti,Patel","105,Purvesh,Dave","104,Miti,Patel"]
enrollmentDf = sc.parallelize(enrollmentList).map(lambda trans:trans.split(",")).map(lambda r:studentEnrollment(*r)).toDF()
enrollmentDf.show()
suppliers = spark.sql("select * from miti_db.employee")
print('count of suplier {0}'.format(suppliers.count()))
dropduplicatsupplier=suppliers.dropDuplicates()
print('count of dinstinct supplier {0}'.format(dropduplicatsupplier.count()))
quit()
