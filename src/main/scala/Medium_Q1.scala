import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Medium_Q1 {

  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder
      .appName("Spark_program")
      .master("local[*]")
      .getOrCreate()

    val schema=" Item_id Int,quantity Int"

    val df=spark.read
      .format("csv")
      .option("Header",true)
      .schema(schema)
      .option("path","C:/Users/divya/Downloads/Medium_Q1.csv")
      .load()

    df.show()

    df.select(col("Item_id"),col("quantity"),when(col("quantity")<10,"Low").when(col("quantity")>=10 && col("quantity")<=20,"Medium")
    .otherwise("High").alias("stock_level")).show()

  }

}
