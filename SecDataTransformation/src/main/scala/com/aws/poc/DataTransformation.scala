package com.aws.poc


import org.apache.spark
import org.apache.spark.sql.SparkSession
import spark.sql


object DataTransformation {

  def main(args: Array[String]): Unit = {
    println("Spark Application Started................")

    //create spark session
    val spark = SparkSession.builder().appName(name = "Data Tansformation").enableHiveSupport().getOrCreate()

   
    val submissionDataset = spark.read.format("csv").option("delimiter","\t").option("inferSchema","true").option("header","true").load("s3n://ingestion-user-s3-bucket/Raw-Zone/SEC-Data/submission/*.txt")
    val numbersDataset = spark.read.format("csv").option("delimiter","\t").option("inferSchema","true").option("header","true").load("s3n://ingestion-user-s3-bucket/Raw-Zone/SEC-Data/numbers/*.txt")
    val sic_code = spark.read.format("csv").option("delimiter","\t").option("inferSchema","true").option("header","true").load("s3n://ingestion-user-s3-bucket/Raw-Zone/SEC-Data/sic_code_industry.txt")
    
    //Creating temp view to preserve original ds
    submissionDataset.createOrReplaceTempView("sub")
    numbersDataset.createOrReplaceTempView("num")
    sic_code.createOrReplaceTempView("sic")

    val resultSet =  spark.sql("SELECT sic.sic_code,sic.industry_title,sub.name,num.adsh,num.tag,num.version,num.ddate,num.qtrs,num.uom,num.value from sub left outer JOIN sic ON sic.sic_code = sub.sic left outer JOIN num on sub.adsh=num.adsh where num.tag IN('Assets', 'Liabilities', 'GrossProfit', 'EarningsPerShareBasic')")
   
    resultSet.coalesce(1).write.format("csv").option("header","true").save("s3n://ingestion-user-s3-bucket/Curated-Zone/Sec-Data/Transformation_Demo/")
    resultSet.coalesce(1).write.format("hive").option("path","s3n://ingestion-user-s3-bucket/Curated-Zone/Sec-Data/Transformation_Demo/SecDataTransformation/").mode("overwrite")saveAsTable("SecDataTransformation")
    spark.stop()



  }

}
