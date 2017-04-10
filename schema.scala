//programmaticaly load create a dataframe in Scala
// Create the Schema. 
//The Schema use StrucType which is nothing but Seq[StructField] . 

//load the file that contains the fieldname
Important note use scala way of upload the file(probabibly can be done in python to be followed....)

import scala.io.Source
import org.apache.spark.sql.types._
 import org.apache.spark.sql.Row

val myFile = "test.txt"
val Contents = Source.fromFile(myFile).getLines.mkString

##Create the collection of structField
val fields = Contents.split(" ").map(fieldName => StructField(FieldName, StringType, nullable=true))

// create the schema
val schema = StructType(fields)
//val rows = noheaders.map(_.split(",")).map(a => Row.fromSeq(a))
/*Ref : https://spark.apache.org/docs/2.0.2/api/java/org/apache/spark/sql/types/StructType.html
#http://spark.apache.org/docs/latest/sql-programming-guide.html#programmatically-specifying-the-schema
#http://alvinalexander.com/scala/scala-how-open-read-files-scala-examples

*/

//upload the file

val rawRDD =spark.sparkContext.textFile("path_tofile")

//create a collection of rows
val rows = rawRDD.map(_.split(",")).map(a => Row.fromSeq(a))
//create the dataframe
val dframe = = spark.createDataFrame(rows, schema)

/* different approach
*/

spark2-shell --packages com.databricks:spark-csv_2.10:1.5.0
import scala.io.Source
 import org.apache.spark.sql.types._

 import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext
val sqlContext = new SQLContext(sc)
val df = sqlContext.read.format("com.databricks.spark.csv")
  .option("delimiter","\\t")
  .option("header", "false")
  .schema(schema)
  .load("path_to_data")
val dfcache = df.cache
//val df10. dfcache.take(10)
val thetable = df.createOrReplaceTempView("dframe")
val res = spark.sql("select * from dframe")
//doing research on caching
