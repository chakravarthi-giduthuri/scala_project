import org.apache.spark.sql.{SparkSession, functions}

object TotalMarksAchievedByStudents {
  def main(args:Array[String]):Unit={
    val spark = SparkSession.builder()
      .appName("total marks achieved by Female and Male students")
      .master("local")
      .getOrCreate()
    val df = spark.read.option("header",value = true).csv("src/test/resources/StudentData.csv")
    df.groupBy(df("gender")).agg(functions.sum(df("marks"))as "total marks by male and female students").show()

  }

}
