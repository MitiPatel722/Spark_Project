df=spark.sql("select * from miti_db.students")
#for colname in df.columns:
#print(df.columns)
for i in df.columns:
	#print(i)
        cols="df.{}".format(i)
       # print(cols);
      # 	df.filter(cols.isNotNull()).count()
	df1=spark.sql("select count({}) from miti_db.students".format(i))
	df2=spark.sql("select count(distinct {}) from miti_db.students".format(i))
	if df1==df2:
		print "yes"
	else:
		print "no"
columns = [ column for column in df.columns if len(df.select(column).distinct().collect()) == len(df.select(column).collect()) ]
df.select(columns).show()
printschea=df.select(columns)
print(printschea)
