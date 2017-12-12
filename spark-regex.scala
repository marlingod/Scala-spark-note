    import org.apache.spark.sql.types._
    val dfResp = spark.read.parquet(sourcepath")

    // create the case class
    //case class ProfileClass (val quoteid: String, val profileValue:Seq[String],val lisValue :Seq[String],val mrvValue: Seq[String], val priorValue: Seq[String], val creditValue: Seq[String],val cedeValue: Seq[String], val rejectValue : Seq[String])
    val dfReasons = dfResp.where("reasons is not null").where("quote_id is not null").select("quote_id", "reasons.reason").distinct()
    val profileNumber="<ProfileNumber[^>]*>(.+?)</ProfileNumber>".r // regex for te
    val LISUsed = "<LISUsed[^>]*>(.+?)</LISUsed>".r
    val MVRUsed = "<MVRUsed[^>]*>(.+?)</MVRUsed>".r
    val PriorInsUsed = "<PriorInsUsed[^>]*>(.+?)</PriorInsUsed>".r
    val CreditUsed = "<CreditUsed[^>]*>(.+?)</CreditUsed>".r
    val cedeCode =  "<CedeCode[^>]*>(.+?)</CedeCode>".r
    val RejectFrom = "<RejectFrom[^>]*>(.+?)</RejectFrom>".r
    //partern searching
    val profileReg = "[0-9]+".r // get the profile number matchin
    val lisuedReg = "[N|Y]".r // get the element of lisued
    val mvusedReg = "[N|Y]".r // get the elements of mvused
    val priorReg = "[N|Y]".r  // get the value fo the pri
    val creditReg = "[N|Y]".r  // get the value fo the pri
    val rejecReg = "(([0-9]{3})/([0-9]{3}))".r //get the element of the reject form
    val CeceReg = "[0-9]".r // ge the value of tsample he cedeCode

    val dfProfile = dfReasons.map{line => {val quoteidR = profileNumber.findAllIn(line.toString).toList; // get the profile Tag
      val profileValue = profileReg.findAllIn(quoteidR.toString).toList; //gget the profile value
      val lisued = LISUsed.findAllIn(line.toString).toList;   // get the lisued tag
      val lisValue = lisuedReg.findAllIn(lisued.toString).toList; //get the lisued value
      val mrused = MVRUsed.findAllIn(line.toString).toList; //get the mrvused tag
      val mrvValue = mvusedReg.findAllIn(mrused.toString).toList; //ge the mrused value
      val prior = PriorInsUsed.findAllIn(line.toString).toList; //get the prior used tag
      val priorValue = priorReg.findAllIn(prior.toString).toList; // get the prior used value
      val credit = CreditUsed.findAllIn(line.toString).toList; //get the credit used tag
      val creditValue = creditReg.findAllIn(credit.toString).toList; // get the credit value
      val cede = cedeCode.findAllIn(line.toString).toList; //get the cede code tag
      val cedeValue = CeceReg.findAllIn(cede.toString).toList; // get the cedeCode value
      val reject = RejectFrom.findAllIn(line.toString).toList; //get the rejectFrom tag
      val rejectValue = rejecReg.findAllIn(reject.mkString("")).toList; // get the reject from value
      (line(0).toString,profileValue,lisValue,mrvValue,priorValue, creditValue,cedeValue, rejectValue )}}

    val dfProf = dfProfile.toDF("quoteid", "profileNumber", "lisUsed", "Mvrused", "PriorInsUsed","CreditUsed","cedeCode","RejectFrom")
  //explode the coolumn
    val explodedf = dfProf.withColumn("profileEx", explode(col("profileNumber")))
      .withColumn("lisUsedEx", explode(col("lisUsed")))
      .withColumn("MvrusedEx", explode(col("Mvrused")))
      .withColumn("PriorInsUsedEx", explode(col("PriorInsUsed")))
      .withColumn("CreditUsedEx", explode(col("CreditUsed")))
      .withColumn("cedeCodeEx", explode(col("cedeCode")))
      .withColumn("RejectFromEx", explode(col("RejectFrom")))
    //create a temp table and do some sql
//explodedf.cache()
explodedf.createOrReplaceTempView("thetable")
    val profileFinal =explodedf.select($"quoteid".alias("quote_id").cast(LongType),$"profileEx".alias("profileNumber"),$"lisUsedEx", $"MvrusedEx",$"PriorInsUsedEx", $"CreditUsedEx",$"cedeCodeEx",$"RejectFromEx").distinct()   //val profileFinal = spark.sql("select distinct quoteid ,profileEx,lisUsedEx ,MvrusedEx , PriorInsUsedEx, CreditUsedEx , cedeCodeEx , RejectFromEx " +
    //profileFinal.write.mode("overwrite").save(destfolder)
    //dfProf.write.mode("overwrite").save(destfolder)
    profileFinal.write.mode("overwrite").saveAsTable("<db>.<table>")
