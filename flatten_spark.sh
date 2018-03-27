pyspark2 <<EOF
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("NorthwindDenormalizer")
sc = SparkContext(conf=conf)
from pyspark.sql import HiveContext
#hive_context = HiveContext(sc)
spark.sql("select * from  miti_db. salesorderdetail")
EOF
