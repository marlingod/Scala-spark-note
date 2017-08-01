# Scala-spark-note
Scala Notes and Spark
## Run spark app:
spark-submit
--master yarn \
--class com.cnn.Package.scalaObject \
--deploy-mode cluster \
--files <local file_path>  \(to be loaded with the application)
<app>.jar \
--file_name 
## connect to S3 
  ### using sparkContext as sc
sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "key_value") 
sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "secret_key_value")
  
  val theRDD = sc.textFile("s3n://bucketname/filename")
  ### using the sparkSession
spark.conf.set("fs.s3n.awsAccessKeyId", "key_value")
spark.conf.set("fs.s3n.awsSecretAccessKey", "secret_key_value")

//the data
val myRDD2 = spark.read.textFile("s3n://nm5test3/chord.txt")
