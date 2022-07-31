import org.apache.spark.sql.SparkSession

object TotalNoOfStudents {
  def main(args:Array[String]):Unit={
    val spark = SparkSession.builder()
      .appName("total no of students in the file")
      .master("local")
      .getOrCreate()

    val df = spark.read.option("header",value = true).csv("src/test/resources/StudentData.csv")
    df.printSchema()
    df.head(10).foreach(println)
    println("total no of students in the file: "+df.count())
  }


}
