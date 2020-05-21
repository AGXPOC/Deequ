import com.amazon.deequ.{VerificationSuite, VerificationResult}
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.checks.{Check, CheckLevel}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


val format = "yyyyMMdd_HHmmss"
val dtf = DateTimeFormatter.ofPattern(format)
val ldt = LocalDateTime.now()
val currentTime = ldt.format(dtf)

val outputDirSub = "s3n://ingestion-user-s3-bucket/result/SEC-Data/submission/ver/"+currentTime
val outputDirTag = "s3n://ingestion-user-s3-bucket/result/SEC-Data/tag/ver/"+currentTime
val outputDirNum = "s3n://ingestion-user-s3-bucket/result/SEC-Data/numeric/ver/"+currentTime
val outputDirPre = "s3n://ingestion-user-s3-bucket/result/SEC-Data/presentation/ver/"+currentTime



val submissionDataset = spark.read.format("csv").option("delimiter","\t").option("inferSchema","true").option("header","true").load("s3n://ingestion-user-s3-bucket/data/SEC-Data/submission/*.txt")
val presentationDataset = spark.read.format("csv").option("delimiter","\t").option("inferSchema","true").option("header","true").load("s3n://ingestion-user-s3-bucket/data/SEC-Data/presentation/*.txt")
val numbersDataset = spark.read.format("csv").option("delimiter","\t").option("inferSchema","true").option("header","true").load("s3n://ingestion-user-s3-bucket/data/SEC-Data/numbers/*.txt")
val tagDataset = spark.read.format("csv").option("delimiter","\t").option("inferSchema","true").option("header","true").load("s3n://ingestion-user-s3-bucket/data/SEC-Data/tag/*.txt")

val verificationResultSub: VerificationResult = { VerificationSuite().onData(submissionDataset)
  .addCheck(
    Check(CheckLevel.Error, "Review Check")
      .isComplete("nciks")
      .isNonNegative("nciks")
      .isComplete("name")
      .isComplete("accepted")
      .isComplete("detail")
      .isNonNegative("filed")
      .isContainedIn("afs", Array("1-LAF", "4-NON", "2-ACC", "5-SML", "3-SRA"))
      .isComplete("zipba"))
      .run()
}

// convert check results to a Spark data frame
val resultDataFrameSub = checkResultsAsDataFrame(spark, verificationResultSub)
resultDataFrameSub.write.format("csv").option("header","false").save(outputDirSub)


//tag Data
val verificationResultTag: VerificationResult = { VerificationSuite().onData(tagDataset)
  .addCheck(
    Check(CheckLevel.Error, "Review Check")
      .isComplete("nciks")
      .isNonNegative("nciks")
      .isContainedIn("datatype", Array("monetary", "member", "shares")))
      .run()
}

// convert check results to a Spark data frame
val resultDataFrameTag = checkResultsAsDataFrame(spark, verificationResultTag)
resultDataFrameTag.write.format("csv").option("header","false").save(outputDirTag)


//Presentation Data
val verificationResultPre: VerificationResult = { VerificationSuite().onData(presentationDataset)
  .addCheck(
    Check(CheckLevel.Error, "Review Check")
      .hasMin("report", _ == 3) // min is 1.0
      .hasMax("report", _ == 9)
      .isContainedIn("rfile", Array("H")))
      .run()
}

// convert check results to a Spark data frame
val resultDataFramePre = checkResultsAsDataFrame(spark, verificationResultPre)
resultDataFramePre.write.format("csv").option("header","false").save(outputDirPre)


//Numeric Data
val verificationResultNum: VerificationResult = { VerificationSuite().onData(numbersDataset)
  .addCheck(
    Check(CheckLevel.Error, "Review Check")
      .isComplete("ddate")
      .isNonNegative("ddate")
      .isComplete("tag")
      .isComplete("value")
      .isContainedIn("qtrs", Array("1", "2", "3", "4")))
      .run()
}

// convert check results to a Spark data frame
val resultDataFrameNum = checkResultsAsDataFrame(spark, verificationResultNum)
resultDataFrameNum.write.format("csv").option("header","false").save(outputDirNum)