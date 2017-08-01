# Scala-spark-note
Scala Notes and Spark
## Run spark app:
spark-submit
--master yarn
--class com.cnn.Package.scalaObject
--deploy-mode cluster
--files <local file_path> (to be loaded with the application)
<app>.jar
--file_name 
