import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Complex_Q5 {
  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder
      .appName("Spark-Program")
      .master("local[*]")
      .getOrCreate()
    val schema="order_id Int,quantity Int,total_price Int"

    val df=spark.read
      .format("csv")
      .option("Header",true)
      .schema(schema)
      .option("path","C:/Users/divya/Downloads/Complex_Q5.csv")
      .load()
    df.show()

    df.show()

    df.select(col("order_id"),col("quantity"),col("total_price"),when(col("quantity")<10 && col("total_price")<200,"Small & Cheap")
    .when(col("quantity")>=10 && col("total_price")<200,"Bulk & Discounted")
    .otherwise("Premium Order").alias("order_type")).show()
  }

}
