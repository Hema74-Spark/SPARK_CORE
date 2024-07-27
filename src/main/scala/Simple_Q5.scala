import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Simple_Q5 {

  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder
      .appName("Spark-Program")
      .master("local[*]")
      .getOrCreate()

    val schema=" event_id Int, date Date"

    val df=spark.read
      .format("csv")
      .option("Header",true)
      .schema(schema)
      .option("path","C:/Users/divya/Downloads/Simple_Q5.csv")
      .load()

    df.show(5,false)

    df.select(col("event_id"),col("date"),
      when(col("date")==="2024-12-25","true").when(col("date")=== "2025-01-01","true").otherwise("false").alias("is_holiday")).show()
  }

}
