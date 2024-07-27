import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}

object Simple_Q4 {

  def main(args:Array[String]):Unit={
    val spark =SparkSession.builder
      .appName("Spark-Program")
      .master("local[*]")
      .getOrCreate()

    val schema=" product_id Int,Price Double"

    val df=spark.read
      .format("csv")
      .option("Header",true)
      .schema(schema)
      .option("Path","C:/Users/divya/Downloads/Simple_Q4.csv")
      .load()
    df.show(5,false)

    df.select(col("product_id"),col("Price"),
      when(col("Price")<50,"Cheap").when(col("Price")>=50 && col("Price")<=100,"Moderate").otherwise("Expensive").alias("price_range"))
      .show()
  }

}
