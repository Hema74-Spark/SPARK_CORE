import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Complex_Q7 {

  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder
      .appName("spark-program")
      .master("local[*]")
      .getOrCreate()

    val schema="student_id Int,math_score Int,english_score Int"

    val df=spark.read
      .format("csv")
      .option("Header",true)
      .schema(schema)
      .option("path","C:/Users/divya/Downloads/Complex_Q7.csv")
      .load()
df.show()
    df.select(col("student_id"),col("math_score"),col("english_score"),when(col("math_score")>80,"A")
    .when(col("math_score").between(60,80),"B").otherwise("C").alias("math_grade"),when(col("english_score")>80,"A")
    .when(col("english_score").between(60,80),"B").otherwise("C").alias("english_grade")).show()
  }

}
