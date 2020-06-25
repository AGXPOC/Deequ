package com.aws.poc

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset

import com.amazon.deequ.{VerificationSuite, VerificationResult}
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.checks.{Check, CheckLevel}
//import spark.implicits._ // for toDS method


object DeequVerification {
 
    def submission(spark:SparkSession, currentTime:String)
    {   val currentTimeS = currentTime
        println(currentTimeS)
        val submissionDataset = spark.read.format("csv").option("delimiter","\t").option("inferSchema","true").option("header","true").load("s3n://ingestion-user-s3-bucket/Raw-Zone/SEC-Data/submission/*.txt")
        val outputDir = "s3n://ingestion-user-s3-bucket/Curated-Zone/Sec-Data/submission/verification/"+currentTimeS
        constraintVerificationSub(submissionDataset,outputDir,spark)
    }

    def numbers(spark:SparkSession,currentTime:String)
    {   val currentTimeN = currentTime
        val numbersDataset = spark.read.format("csv").option("delimiter","\t").option("inferSchema","true").option("header","true").load("s3n://ingestion-user-s3-bucket/Raw-Zone/SEC-Data/numbers/*.txt")
        val outputDir = "s3n://ingestion-user-s3-bucket/Curated-Zone/Sec-Data/numbers/verification/"+currentTimeN
        constraintVerificationNum(numbersDataset,outputDir,spark)
    }

  
    def constraintVerificationSub(dataset:Dataset[Row],outputDir:String,spark:SparkSession)
    {
      import spark.implicits._
    val verificationResult: VerificationResult = { VerificationSuite().onData(dataset)
                                                      .addCheck(
                                                     Check(CheckLevel.Error, "Review Check")
                                                       .isComplete("adsh")
                                                       .isComplete("name"))
                                                       .run()
                                                     }
  val resultDataFrame = checkResultsAsDataFrame(spark, verificationResult)
  resultDataFrame.write.format("csv").option("header","false").save(outputDir)
  val hiveDir = outputDir+"hiveTables"
  val df=resultDataFrame.withColumnRenamed("Review Check","ReviewCheck").withColumnRenamed("Input data does not include column nciks!","NoColNicks").withColumnRenamed("CompletenessConstraint(Completeness(nciks,None))","CompletenessConstraint")
  df.coalesce(1).write.format("hive").option("path",hiveDir).mode("overwrite")saveAsTable("DeequVerification")
  
   }

   def constraintVerificationNum(dataset:Dataset[Row],outputDir:String,spark:SparkSession)
    {
      import spark.implicits._
    val verificationResult: VerificationResult = { VerificationSuite().onData(dataset)
                                                      .addCheck(
                                                     Check(CheckLevel.Error, "Review Check")
                                                       .isComplete("adsh")
                                                       .isComplete("tag")
                                                       .isComplete("version")
                                                       .isComplete("ddate")
                                                       .isNonNegative("ddate")
                                                       .isComplete("qtrs")
                                                       .isNonNegative("qtrs")
                                                       .isComplete("uom"))
                                                       .run()
                                                     }
  val resultDataFrame = checkResultsAsDataFrame(spark, verificationResult)
  resultDataFrame.write.format("csv").option("header","false").save(outputDir)
  val hiveDir = outputDir+"hiveTables"
  val df=resultDataFrame.withColumnRenamed("Review Check","ReviewCheck").withColumnRenamed("Input data does not include column nciks!","NoColNicks").withColumnRenamed("CompletenessConstraint(Completeness(nciks,None))","CompletenessConstraint")
  df.coalesce(1).write.format("hive").option("path",hiveDir).mode("overwrite")saveAsTable("DeequVerification")
  
   }


    // Main Method  
    def main(args: Array[String])  
    { 
     val spark = SparkSession.builder().appName("Deequ Verification").enableHiveSupport().getOrCreate()
   // import spark.implicits._
    val format = "yyyyMMdd_HHmmss"
    val dtf = DateTimeFormatter.ofPattern(format)
    val ldt = LocalDateTime.now()
    val currentTime = ldt.format(dtf)
    
     
     submission(spark,currentTime)
     numbers(spark,currentTime)

     spark.stop()
        
    }
  
}