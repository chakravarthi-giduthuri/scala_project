import org.apache.spark.sql.SparkSession

object PassOrFail {
  def main(args:Array[String]):Unit={
    val spark = SparkSession.builder()
      .appName("total marks achieved by Female and Male students")
      .master("local")
      .getOrCreate()
    val df = spark.read.option("header",value = true).csv("src/test/resources/StudentData.csv")

    val filterpass = df.filter(df("marks")>50).count()
    val filterfail = df.filter(df("marks")<=50).count()
    println("total passed students: "+filterpass)
    println("total failed students: "+filterfail)
  }


}
