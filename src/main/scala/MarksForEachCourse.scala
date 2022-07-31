import org.apache.spark.sql.{SparkSession, functions}

object MarksForEachCourse {
  def main(args:Array[String]):Unit={
    val spark = SparkSession.builder()
      .appName("total marks for each course")
      .master("local")
      .getOrCreate()
    val df = spark.read.option("header",value = true).csv("src/test/resources/StudentData.csv")
    val total = df.groupBy(df("course")).agg(functions.sum(df("marks")as "total marks for each course"))
    total.show()
  }
}
