import pyspark
import findspark
from pyspark.sql import SparkSession

findspark.add_packages('mysql:mysql-connector-java:8.0.22')

spark = SparkSession.builder.appName("Python Spark SQL basic example").config("master", "local[*]").getOrCreate()

#jdbcDF = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/mytestdb") \
#      .option("driver", 'com.mysql.jdbc.Driver') \
#      .option("dbtable", "mytestdb.Pythontest3") \
#      .option("user", "root")   \
#      .option("password", "root")  \
#      .option("numPartitions", 10)    \
#      .option("partitionColumn", "doj")   \
#      .option("lowerBound", "2020-01-01 00:00:00")  \
#      .option("upperBound", "2020-12-01 00:00:00")   \
#      .option("timestampFormat", "yyyy-mm-dd hh:mm:ss")   \
#      .load()


jdbcDF = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/mytestdb") \
      .option("driver", 'com.mysql.jdbc.Driver') \
      .option("dbtable", "mytestdb.Pythontest3") \
      .option("user", "root")   \
      .option("password", "root")  \
      .option("numPartitions", 10)    \
      .option("partitionColumn", "doj")   \
      .option("oracle.jdbc.mapDateToTimestamp","false") \
      .option("lowerBound", "2019-01-01" ) \
      .option("upperBound", "2020-05-01") \
      .option("dateFormat", "YYYY-MM-DD" ) \
      .load()

jdbcDF.show()
print(jdbcDF.rdd.getNumPartitions())

