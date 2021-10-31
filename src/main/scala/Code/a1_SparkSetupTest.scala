package Code

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object a1_SparkSetupTest {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
 def main(args:Array[String])
 {
   val spark = SparkSession.builder()
    .appName("Hello Spark SQL")
    .master("local[3]")
    .getOrCreate()
  val surveyDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/01/sample.csv")

  surveyDF.createOrReplaceTempView("survey_tbl")
  surveyDF.show()
  val countDF = spark.sql("select Country, count(1) as count from survey_tbl where Age<40 group by Country")

  logger.info(countDF.collect().mkString("->"))
  //scala.io.StdIn.readLine()
  spark.stop()
}
}
