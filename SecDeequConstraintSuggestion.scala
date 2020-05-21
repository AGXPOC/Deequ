import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}
import spark.implicits._ // for toDS method

val format = "yyyyMMdd_HHmmss"
val dtf = DateTimeFormatter.ofPattern(format)
val ldt = LocalDateTime.now()
val currentTime = ldt.format(dtf)

val outputDirSub = "s3n://ingestion-user-s3-bucket/result/SEC-Data/submission/"+currentTime
val outputDirTag = "s3n://ingestion-user-s3-bucket/result/SEC-Data/tag/"+currentTime
val outputDirNum = "s3n://ingestion-user-s3-bucket/result/SEC-Data/numeric/"+currentTime
val outputDirPre = "s3n://ingestion-user-s3-bucket/result/SEC-Data/presentation/"+currentTime


val submissionDataset = spark.read.format("csv").option("delimiter","\t").option("inferSchema","true").option("header","true").load("s3n://ingestion-user-s3-bucket/data/SEC-Data/submission/*.txt")
val presentationDataset = spark.read.format("csv").option("delimiter","\t").option("inferSchema","true").option("header","true").load("s3n://ingestion-user-s3-bucket/data/SEC-Data/presentation/*.txt")
val numbersDataset = spark.read.format("csv").option("delimiter","\t").option("inferSchema","true").option("header","true").load("s3n://ingestion-user-s3-bucket/data/SEC-Data/numbers/*.txt")
val tagDataset = spark.read.format("csv").option("delimiter","\t").option("inferSchema","true").option("header","true").load("s3n://ingestion-user-s3-bucket/data/SEC-Data/tag/*.txt")


// We ask deequ to compute constraint suggestions for us on the data
val suggestionResultSub = { ConstraintSuggestionRunner().onData(submissionDataset).addConstraintRules(Rules.DEFAULT).run()}

val suggestionDataFrameSub = suggestionResultSub.constraintSuggestions.flatMap { 
  case (column, suggestions) => 
    suggestions.map { constraint =>(column, constraint.description, constraint.codeForConstraint)} 
}.toSeq.toDS()

//suggestionDataFrame.show(truncate=false)

suggestionDataFrameSub.write.format("csv").option("header","false").save(outputDirSub)

//Tag Data
val suggestionResultTag = { ConstraintSuggestionRunner().onData(tagDataset).addConstraintRules(Rules.DEFAULT).run()}

val suggestionDataFrameTag = suggestionResultTag.constraintSuggestions.flatMap { 
  case (column, suggestions) => 
    suggestions.map { constraint =>(column, constraint.description, constraint.codeForConstraint)} 
}.toSeq.toDS()

suggestionDataFrameTag.write.format("csv").option("header","false").save(outputDirTag)

//Numeric Data
val suggestionResultNum = { ConstraintSuggestionRunner().onData(numbersDataset).addConstraintRules(Rules.DEFAULT).run()}

val suggestionDataFrameNum = suggestionResultNum.constraintSuggestions.flatMap { 
  case (column, suggestions) => 
    suggestions.map { constraint =>(column, constraint.description, constraint.codeForConstraint)} 
}.toSeq.toDS()

suggestionDataFrameNum.write.format("csv").option("header","false").save(outputDirNum)

//Presentation Data
val suggestionResultPre = { ConstraintSuggestionRunner().onData(presentationDataset).addConstraintRules(Rules.DEFAULT).run()}

val suggestionDataFramePre = suggestionResultPre.constraintSuggestions.flatMap { 
  case (column, suggestions) => 
    suggestions.map { constraint =>(column, constraint.description, constraint.codeForConstraint)} 
}.toSeq.toDS()

suggestionDataFramePre.write.format("csv").option("header","false").save(outputDirPre)

