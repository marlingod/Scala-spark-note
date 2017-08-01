//this application get the schema file from local directory. Create a StructType from the data.
//read new file in the directory every 30 mn.
//select some field and run Stream ETL .

import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.types.{StringType, ArrayType, StructType}
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.streaming.Trigger
import scala.io.Source
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql._
import scala.concurrent.duration._
import org.apache.spark.sql.functions._

object webNmglo {
   def main(args: Array[String]): Unit = {
     val spark = SparkSession.builder().getOrCreate()
     import spark.implicits._

     val filename = args(0)
     //val Contents = Source.fromFile(schema_file1).getLines.mkString

     val Contents = Source.fromFile(filename).getLines.mkString

     //create the collection of StructField
     val fields = Contents.split("\t").map(fieldName => StructField(fieldName, StringType, nullable = true))
     //create the schema
     val schema = StructType(fields)

     //get the folder where all the file are being landed
     val folder_path = <path_tofile_load> 

     //read the files into DataFrame
     val rawDf = spark.readStream.format("csv").option("delimiter", "\t").option("header", false).schema(schema).load(folder_path)

     val recordsEvent = rawDf.select(unix_timestamp($"DATE_TIME", "yyyy-MM-dd' 'hh:mm:ss").cast("timestamp") as "times",
       expr("accept_language as accept_language"),
       expr("browser as web_browser"),
       expr("carrier as carrier"),
       expr("connection_type as connection_type"),
       ....
     )

     // drop the null value
     /*
  Passing in “any” as an argument will drop a row if any of the values are null. Passing in “all” will only drop the row if all values are null or NaN for that row.
  */
    // val recordsNonNa = recordsEvent.na.drop("all", Seq("times"))

     // check point and process data folders

     val checkpointPath = <path_to_checkpoint>
     val folder_parquet = <path_to_parquet_folder>

     // write the transform data into process data folder. poll the landing folder and run the query every 30 mn

     val streamingETLQuery = recordsEvent.withColumn("dateof", $"times".cast("date"))
       .writeStream
       .trigger(ProcessingTime(30.minutes))
       .format("parquet").partitionBy("dateof")
       .option("path", folder_parquet)
       .option("checkpointLocation", checkpointPath)
       //.outputMode("append")
       .start()
     streamingETLQuery.awaitTermination()
     spark.stop()
   }
}
