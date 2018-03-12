import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalatest.{FunSpec, FunSuite}
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.rdd.RDD
trait SparkSessionTestWrapper {
  lazy val spark: SparkSession = {
    SparkSession.builder().master("local").appName("MovingAverage").getOrCreate()
  }
}
class MovingAvgTesting extends FunSpec with RDDComparisons  with SparkSessionTestWrapper {

  import spark.implicits._

  describe("testing on the moving average using list") {
    it("testing the first dataset") {
      val inputRDD = spark.sparkContext.parallelize(1 to 100)
      val expectedRDD = spark.sparkContext.parallelize(2 to 99)
      val actualRDD = inputRDD.sliding(3).map(cal => (cal.sum / cal.size))
      assert(None === compareRDD(expectedRDD, actualRDD))
    }

  }
}
