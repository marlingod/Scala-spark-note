import scala.xml.XML.loadString
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import scala.util.Try
import org.apache.spark.storage.StorageLevel

//spark tuning
//spark2-shell --num-executors 40 --executor-cores 6 --executor-memory 20G --jars spark-xml_2.11-0.4.1.jar

//val df =spark.read.format("xml").option("rowtag", "LinePlacementDecision").option("rootTag", "LinePlacementDecision").load(sourcepath)
// load teh data
val profilePattern = "<ProfileDesc[^>]*>(.+?)</ProfileDesc>".r
val adviceListPattern ="<AdviceList[^>]*>(.+?)</AdviceList>".r
var sxd = ArrayBuffer[String]()
val df = spark.read.csv(sourcepath)

// select the quote and the state
val dfQuoteState  = df.select("_c1", "_c2").withColumnRenamed("_c1", "quoteId").withColumnRenamed("_c2", "StateCd")

//select the response xml.

val dfResponse =  df.select("_c4").where("_c4 is not null")

 val dfNoadvice = dfResponse.map{ line =>  {val noPro =profilePattern.replaceAllIn(line.toString, ""); val noAdvice = adviceListPattern.replaceAllIn(noPro, ""); noAdvice}}

//check the number of parition and repartition
dfResponse.rdd.getNumPartitions

val dfResRep = dfResponse.repartition(4)
/*
def removeAdv(array: Array[String]): Array[String] = {
  array.map { line => {
    val NoPro = profilePattern.replaceAllIn(line, "")
    val noAdvice = adviceListPattern.replaceAllIn(NoPro, "")
    val thexml = loadString(noAdvice)
    val myxml = (thexml \ "ProfileNumber").text
    myxml
  }
  }
}
*/


case class LPD (val quoteId : String, company: String, val line: String, val reasons: Seq[Reasons])
case class Reason(val Profile: String, val CreditUsed: String, val LIS: String,val MVR : String, val PriorIns: String, val RejectFrom : String, val Cede: String )
case class Reasons(val reasons: Seq[Reason])

//function to parse the xml using the xml

def xmlParseLP(lpxml: scala.xml.Elem) ={
  //val lpx = loadString(lpxml.toString)
  lpxml.map(the=> {
  val company = (lpxml \ "company").text;
  val quoteId = (lpxml \ "quote_id").text;
  val line = (lpxml \ "line").text;
  val reasons = (lpxml \ "reasons").map(thereasons => {
    val reason = (thereasons \\ "reason").map(thereason => {
      //val profileID = (thereason \ "ProfileDesc").text.mkString("");
      val profile = (thereason \ "Profile").text.mkString("")
      val Credit = (thereason \ "Credit").text.mkString("")
      val LIS = (thereason \ "LIS").text.mkString("")
      val MVR = (thereason \ "MVR").text.mkString("")
      val PriorIns = (thereason \ "PriorIns").text.mkString("") //RejectFrom
      val Reject = (thereason \ "RejectFrom").text.mkString("")
      val Cede = (thereason \ "Cede").text.mkString("")
      Reason(profile , Credit , LIS, MVRUsed, PriorIns, Reject, Cede)
    })
    Reasons(reason)
  })
  LPD(quoteId,company, line,reasons)
})
}

val resXML   = dfNoadvice.rdd
//val resp2    = resXML.map{line => try {(loadString(line.mkString.replace("\"<","<").replace(">\"",">").replace("\\","")))}finally{sxd += line}}
val resp2    = resXML.map{line => try {(loadString(line.mkString.replace("\"<","<").replace(">\"",">").replace("\\","")))}catch{case e: Exception => println("exception caught: " + e)}}

val thefinal = resp2.filter(line => line.getClass.getSimpleName =="Elem").flatMap{elt => xmlParseLP(loadString(elt.toString))} //.persist(StorageLevel.MEMORY_AND_DISK_SER)
val finDF    = thefinal.toDF

//val dfrdd = dfResponse.rdd.map{ line =>{val NoPro = profilePattern.replaceAllIn(line, ""); val noAdvice = adviceListPattern.replaceAllIn(NoPro, ""); noAdvice}}

finDF.write.mode("overwrite").save(destfolder)
