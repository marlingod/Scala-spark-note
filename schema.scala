#programmaticaly load create a dataframe in Scala
## Create the Schema. 
The Schema use StrucType which is nothing but Seq[StructField] . 

###load the file that contains the fieldname
Important note use scala way of upload the file(probabibly can be done in python to be followed....)

import scala.io.Source
import org.apache.spark.sql.types._

val myFile = "test.txt"
val Contents = Source.fromFile(myFile).getLines.mkString

##Create the collection of structField
val fields = myFile.split(" ").map(fieldName => StructField(FieldName, StringType, nullable=true))

# create the schema
fields = StructType(fields)
#Ref : https://spark.apache.org/docs/2.0.2/api/java/org/apache/spark/sql/types/StructType.html
#http://spark.apache.org/docs/latest/sql-programming-guide.html#programmatically-specifying-the-schema
