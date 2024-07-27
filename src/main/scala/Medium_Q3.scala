import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Month
import org.apache.spark.sql.functions.{col, month, when}

object Medium_Q3 {
  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder
      .appName("Spark-Program")
      .master("local[*]")
      .getOrCreate()

    val schema="order_id Int,order_date Date"

    val df = spark.read
      .format("csv")
      .option("Header",true)
      .schema(schema)
      .option("path","C:/Users/divya/Downloads/Medium_Q3.csv")
      .load()
    df.show()

    df.select(col("order_id"),col("order_date"),when(month(col("order_date"))=== 6 || month(col("order_date"))=== 7 ||
      month(col("order_date"))=== 8,"Summer").when(month(col("order_date"))=== 12 || month(col("order_date"))=== 1 ||
      month(col("order_date"))=== 2,"Winter").otherwise("Other").alias("season")).show()
  }

}
