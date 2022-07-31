import org.apache.spark.sql.{SparkSession, functions}

object MaxAndMinMarks {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Minimum and maximum marks achieved for each course")
      .master("local")
      .getOrCreate()
    val df = spark.read.option("header", value = true).csv("src/test/resources/StudentData.csv")
    val minMarks = df.groupBy(df("course")).agg(functions.min(df("marks") as "Minimum marks for each course"))
    val maxMarks = df.groupBy(df("course")).agg(functions.max(df("marks") as "Maximum marks for each course"))
    minMarks.show()
    maxMarks.show()
  }

}
