import org.apache.spark.sql.SparkSession

object StudentsPerCourse {
  def main(args:Array[String]):Unit={
      val spark = SparkSession.builder()
        .appName("total marks achieved by Female and Male students")
        .master("local")
        .getOrCreate()
      val df = spark.read.option("header",value = true).csv("src/test/resources/StudentData.csv")
      df.groupBy(df("course")).count().show()


    }

}
