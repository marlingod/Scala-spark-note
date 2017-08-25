package ClickStream

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3._
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{S3ObjectSummary, ObjectListing, GetObjectRequest}
import scala.collection.mutable.ListBuffer
import scala.util.matching._
import model._
import scala.collection.JavaConversions.{collectionAsScalaIterable => asScala}
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.types.{StringType, ArrayType, StructType}
import org.apache.spark.sql.streaming._
import scala.io.Source
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql._
import scala.concurrent.duration._
import org.apache.spark.sql.functions._
import java.io._
/**
  * Created by Nick Bustoski
  *
  * how to create an app
  * https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-apache-spark-create-standalone-application
  * the s3 listObject has a default max of 1000 objects
  * https://stackoverflow.com/questions/12853476/amazon-s3-returns-only-1000-entries-for-one-bucket-and-all-for-another-bucket-u
  */


object adobeClick {
  def main(args: Array[String]): Unit ={
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val s3 = new AmazonS3Client(new BasicAWSCredentials(<key>, <secret-key>))

    //set spark connection to S3
    spark.conf.set("fs.s3n.awsAccessKeyId", <key>)
    spark.conf.set("fs.s3n.awsSecretAccessKey", <secret-key>)
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec" , "snappy") //gzip

    //////////////////////////////functions///////////////////////////////////

    // write the function to return the objects list of a bucket:
    def mapObjectsBucket[T](s3: AmazonS3Client, bucket: String, prefix: String)(f: (S3ObjectSummary) => T) = {

      def scan(acc:List[T], listing:ObjectListing): List[T] = {
        val summaries = asScala[S3ObjectSummary](listing.getObjectSummaries())
        val mapped = (for (summary <- summaries) yield f(summary)).toList

        if (!listing.isTruncated) mapped.toList
        else scan(acc ::: mapped, s3.listNextBatchOfObjects(listing))
      }

      scan(List(), s3.listObjects(bucket, prefix))
    }

    //define the function to rerive the filetime of the in the file name:
    def myFiletimeStamp(fileName: String) : String  = {
      val header_pat = """(something)([\d-]*)(.tsv.gz)""".r
      val header_pat(column:String, filetime: String, ext: String ) = fileName
      return filetime
    }
    //define the function to rerive the time in the headers name:
    def myHeadertimeStamp(fileName: String) : String  = {
      val header_pat = """(something_)([\d-]*)(.tsv)""".r
      val header_pat(column:String, filetime: String, ext: String ) = fileName
      return filetime
    }

    //////////////////get the list of file_name and list of headers //////////////////////////
    val bucketFileName   = "something1"
    val bucketHeaderName = "something1"
    val save_folder        = "something1"
    val hdfs_folder = "/path/to/hdfs"

    // define the buffered List for the processed files and headers
    var headers_processed   = new ListBuffer[String]()
    var filePorocessed      = new ListBuffer[String]()
    var fileNotProcessed    = new ListBuffer[String]()
    var FileNames           = new ListBuffer[String]()
    var headerNames         = new ListBuffer[String]()
    //create the lists of file and header using the fnction mapObjectsBucket define above
    mapObjectsBucket(s3, bucketFileName, "")(s => FileNames    +=s.getKey )
    mapObjectsBucket(s3, bucketHeaderName, "")(s => headerNames +=s.getKey )



    //define the path to write the process files
    val headerProcessedPath 	= "/path/to/filet" // path the the save all files headers_processed
    val fileProcessedPath 		= "/path/to/filet"
    val fileNotProcessedPath	= "/path/to/filet"

    //define the writer
    val writerFileP 	= new PrintWriter(new FileOutputStream(new File(fileProcessedPath), true))
    val writerheaders 	= new PrintWriter(new FileOutputStream(new File(headerProcessedPath), true))
    val writerFileNP 	= new PrintWriter(new FileOutputStream(new File(fileNotProcessedPath), true))

    // load the process and check if there is a new objects in the buckets:
    val fileProcessContents = Source.fromFile(fileProcessedPath).getLines.toList
    val fileNotProcessContents = Source.fromFile(fileNotProcessedPath).getLines.toList
    val allFile = fileNotProcessContents:::fileProcessContents
    val fileToProcess = FileNames.filterNot(allFile.toSet)

    ///////////////////// start the process  /////////////////////////////////////////////
    // can use foreach instead of for. To be research later
    if (fileToProcess.length ==0) {
      sys.exit
    } else {
      for (obj <- fileToProcess) {

        spark.conf.set("fs.s3n.awsAccessKeyId", <key>")
        spark.conf.set("fs.s3n.awsSecretAccessKey", "<secrect>")
        val fileTs = myFiletimeStamp(obj)
        val colName = s"column_headers_${fileTs}.tsv"
 
 if (headerNames.find(_ ==colName).isDefined) {

          // Create the Schema
          val headerFile 			= "s3n://"+ bucketHeaderName +"/"+ colName;
          val headFile   			= spark.read.textFile(headerFile).collect
          val fieldCollect	   	= headFile.flatMap(elt => elt.split("\t"))
          val fields             	= fieldCollect.map(fieldName => StructField(fieldName, StringType, nullable=true))
          val headerInfo    		= s"columns for ${fileTs} : ${fieldCollect.length} columns"
          val schema 	   			= StructType(fields)

          // read the file from the S3 files folders
          val folder_path 		= "s3n://"+ bucketFileName +"/"+ obj;
          val save_path 			= "s3n://"+ save_folder
          val rawDf 				= spark.read.format("csv").option("delimiter", "\t").option("header", false).schema(schema).load(folder_path)
          val result_time1 		= rawDf.select($"date_time".cast("timestamp") as "times" , $"*")
          val results 			= result_time1.withColumn("dateof", $"times".cast("date"))
          val theWrite   			= results.write.format("parquet").partitionBy("dateof").mode("append").option("path", hdfs_folder).save

          // save to string
          headers_processed += headerInfo
          filePorocessed   += obj
        }else {
          fileNotProcessed += obj
        }
      }
    }

    // write the processs files and headers in the file
    headers_processed.map(a =>a +"\n").foreach(writerheaders.write)
    filePorocessed.map(a =>a +"\n").foreach(writerFileP.write)
    fileNotProcessed.map(a =>a +"\n").foreach(writerFileNP.write)
    writerFileP.close
    writerheaders.close
    writerFileNP.close

  }
}
