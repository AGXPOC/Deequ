package com.aws.poc

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset

import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}
//import spark.implicits._ // for toDS method

object DeequConstraintSuggestion
{ 
    def submission(spark:SparkSession, currentTime:String)
    {   val currentTimeS = currentTime
        println(currentTimeS)
        val submissionDataset = spark.read.format("csv").option("delimiter","\t").option("inferSchema","true").option("header","true").load("s3n://ingestion-user-s3-bucket/Raw-Zone/SEC-Data/submission/*.txt")
        val outputDir = "s3n://ingestion-user-s3-bucket/Curated-Zone/Sec-Data/submission/suggestion/"+currentTimeS
        constraintSuggestion(submissionDataset,outputDir,spark)
    }

    def numbers(spark:SparkSession,currentTime:String)
    {   val currentTimeN = currentTime
        val numbersDataset = spark.read.format("csv").option("delimiter","\t").option("inferSchema","true").option("header","true").load("s3n://ingestion-user-s3-bucket/Raw-Zone/SEC-Data/numbers/*.txt")
        val outputDir = "s3n://ingestion-user-s3-bucket/Curated-Zone/Sec-Data/numbers/suggestion/"+currentTimeN
        constraintSuggestion(numbersDataset,outputDir,spark)
    }

    def tag(spark:SparkSession,currentTime:String)
    {   val currentTimeTag = currentTime
        val tagDataset = spark.read.format("csv").option("delimiter","\t").option("inferSchema","true").option("header","true").load("s3n://ingestion-user-s3-bucket/Raw-Zone/SEC-Data/tag/*.txt")
        val outputDir = "s3n://ingestion-user-s3-bucket/Curated-Zone/Sec-Data/tag/suggestion/"+currentTimeTag 
        constraintSuggestion(tagDataset,outputDir,spark)
    }

    def presentation(spark:SparkSession,currentTime:String)
    {   val currentTimeP = currentTime
        val presentationDataset = spark.read.format("csv").option("delimiter","\t").option("inferSchema","true").option("header","true").load("s3n://ingestion-user-s3-bucket/Raw-Zone/SEC-Data/presentation/*.txt")
        val outputDir = "s3n://ingestion-user-s3-bucket/Curated-Zone/Sec-Data/presentation/suggestion/"+currentTimeP
        constraintSuggestion(presentationDataset,outputDir,spark)
    }

    def constraintSuggestion(dataset:Dataset[Row],outputDir:String,spark:SparkSession)
    {
      
      import spark.implicits._
     val suggestionResult = { ConstraintSuggestionRunner()
     .onData(dataset)
    .addConstraintRules(Rules.DEFAULT)
    .run()
    }

    val suggestionDataFrame = suggestionResult.constraintSuggestions.flatMap { 
    case (column, suggestions) => 
        suggestions.map { constraint =>
        (column, constraint.description, constraint.codeForConstraint)
      } 
    }.toSeq//.toDS()

    //suggestionDataFrame.toDS().coalesce(1).write.format("csv").option("header","true").save(outputDir)
    val hiveDir = outputDir+"hiveTables"
    suggestionDataFrame.toDS().coalesce(1).write.format("hive").option("path",hiveDir).mode("overwrite")saveAsTable("DeequSuggestion")
  
    }


    // Main Method  
    def main(args: Array[String])  
    { 
     val spark = SparkSession.builder().appName("Deequ Consraint Suggestion").enableHiveSupport().getOrCreate()
   // import spark.implicits._
    val format = "yyyyMMdd_HHmmss"
    val dtf = DateTimeFormatter.ofPattern(format)
    val ldt = LocalDateTime.now()
    val currentTime = ldt.format(dtf)
    
     
     submission(spark,currentTime)
     numbers(spark,currentTime)
     tag(spark,currentTime)
     presentation(spark,currentTime)

     spark.stop()
        
    } 
} 