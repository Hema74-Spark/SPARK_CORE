import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, hour, to_timestamp, when}

object Medium_Q5 {

  def main(args:Array[String]): Unit = {
    val spark=SparkSession.builder
      .appName("Spark-Program")
      .master("local[*]")
      .getOrCreate()

    val schema="login_id Int,login_time String"

    val df=spark.read
      .format("csv")
      .option("Header",true)
      .schema(schema)
      .option("path","C:/Users/divya/Downloads/Medium_Q5.csv")
      .load()

    df.show(5,false)

    df.select(col("login_id"),col("login_time"),when(hour(col("login_time")) < 12,"true").otherwise("false")
    .alias("is_morning")).show()
  }

}
