import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.rdd.RDDFunctions._
object newRAWorks {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._
    val lt = (1 to 100).zipWithIndex
    val rdd = spark.sparkContext.parallelize(lt).repartition(4)
    val colname = Seq("stock", "er")
    val rdf = rdd.toDF(colname:_*)
    rdf.show()
    val win = Window.orderBy("er").rowsBetween(-2,0)
    val x = rdf.select($"stock",avg($"stock").over(win))
    x.show()
   val y  = rdf.withColumn("ada", avg($"stock").over(win))
    y.show(100)

    val e = spark.sparkContext.parallelize(1 to 100,1).sliding(3).map(c =>(c.sum/c.size))
    e.foreach(println)
  }
}
