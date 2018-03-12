
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import org.scalactic._

class MovingAvgTesting extends FunSuite  with BeforeAndAfter {
  var spark : SparkSession = _
  before {
    spark = SparkSession.builder().appName("udf testings")
      .master("local")
      .config("", "")
      .getOrCreate()
  }


  test("asserting the version of the spark"){

    assert(spark.version == "2.2.1")
  }
}
