import org.apache.spark.sql.{SparkSession, functions}

object AvgMarksforEachCourse {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Average marks achieved for each course")
      .master("local")
      .getOrCreate()
    val df = spark.read.option("header", value = true).csv("src/test/resources/StudentData.csv")
    val total = df.groupBy(df("course")).agg(functions.avg(df("marks") as "average marks for each course"))
    total.show()
  }
}
