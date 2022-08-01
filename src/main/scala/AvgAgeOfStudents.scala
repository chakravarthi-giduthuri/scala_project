import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object AvgAgeOfStudents {
  def main(args:Array[String]):Unit= {
    val spark = SparkSession.builder()
      .appName("Average age of male and female students")
      .master("local")
      .getOrCreate()
//    val df = spark.read.option("header", value = true).csv("src/test/resources/StudentData.csv")
////    val avgAge = df.groupBy(df("gender")).agg(functions.avg(df("age") as "Avg age of students"))
//    avgAge.show()
//    df.filter(df("age").isNull).show()
//    println(df.schema)

    val ownSchema = StructType(
      StructField("age",IntegerType,nullable = true)::
        StructField("gender",StringType,nullable = true)::
        StructField("name",StringType,nullable = true)::
        StructField("course",StringType,nullable = true)::
        StructField("roll",IntegerType,nullable = true)::
        StructField("marks",IntegerType,nullable = true)::
        StructField("email",StringType,nullable = true)::Nil)

    val df = spark.read.option("header", value = true).schema(ownSchema).csv("src/test/resources/StudentData.csv")
df.printSchema()
    df.groupBy(df("gender")).avg("age").show()

  }
}
