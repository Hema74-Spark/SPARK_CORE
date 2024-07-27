import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Medium_Q2 {

  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder
      .appName("Spark-Program")
      .master("local[*]")
      .getOrCreate()

    val schema="customer_id Int,email String"

    val df=spark.read
      .format("csv")
      .option("Header",true)
      .schema(schema)
      .option("path","C:/Users/divya/Downloads/Medium_Q2.csv")
      .load()
    df.show()

    df.select(col("customer_id"),col("email"),when(col("email").contains("gmail"),"Gmail").when(col("email").contains("yahoo"),"Yahoo")
    .otherwise("Other").alias("email_provider")).show()
  }

}
