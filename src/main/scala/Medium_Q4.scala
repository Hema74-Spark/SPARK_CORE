import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Medium_Q4 {
  def main(args:Array[String]): Unit = {
    val spark=SparkSession.builder
      .appName("Spark-Program")
      .master("local[*]")
      .getOrCreate()

    val schema="sale_id Int,amount Int"

    val df = spark.read
      .format("csv")
      .option("Header",true)
      .schema(schema)
      .option("path","C:/Users/divya/Downloads/Medium_Q4.csv")
      .load()

    df.show()

    df.select(col("sale_id"),col("amount"),when(col("amount")<200,"0").when(col("amount")>=200 && col("amount")<=1000,"10")
    .otherwise("20").alias("discount")).show()

  }

}
