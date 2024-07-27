import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Complex_Q6 {
  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder
      .appName("Spark-Program")
      .master("local[*]")
      .getOrCreate()

    val schema="day_id Int,temperature Int,humidity Int"
    val df = spark.read
      .format("csv")
      .option("Header",true)
      .schema(schema)
      .option("path","C:/Users/divya/Downloads/Complex_Q6.csv")
      .load()
    df.show()

    df.select(col("day_id"),col("temperature"),col("humidity"),when(col("temperature")>30,"true").otherwise("false").alias("is_hot")
    ,when(col("temperature")>30,"true").otherwise("false").alias("is_humid")).show()
  }

}
