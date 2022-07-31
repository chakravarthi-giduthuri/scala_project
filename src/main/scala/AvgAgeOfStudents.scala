import org.apache.spark.sql.{SparkSession, functions}

object AvgAgeOfStudents {
  def main(args:Array[String]):Unit= {
    val spark = SparkSession.builder()
      .appName("Average age of male and female students")
      .master("local")
      .getOrCreate()
    val df = spark.read.option("header", value = true).csv("src/test/resources/StudentData.csv")
    val avgAge = df.groupBy(df("gender")).agg(functions.avg(df("age") as "Avg age of students"))
    avgAge.show()
  }
}
