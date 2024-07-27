import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Complex_Q8 {

  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder
      .appName("Spark-Program")
      .master("local[*]")
      .getOrCreate()

    val schema="email_id Int,email_address String"

    val df=spark.read
      .format("csv")
      .option("Header",true)
      .schema(schema)
      .option("path","C:/Users/divya/Downloads/Complex_Q8.csv")
      .load()

    df.show()

    df.select(col("email_id"),col("email_address"),when(col("email_address").contains("gmail"),"Gmail")
      .when(col("email_address").contains("yahoo"),"Yahoo").otherwise("HotMail").alias("")).show(false)
  }

}
