import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Complex_Q2 {
def main(args:Array[String]):Unit={
  val spark=SparkSession.builder
    .appName("Spark-pargram")
    .master("local[*]")
    .getOrCreate()
  val schema="review_id Int,Rating Int"

  val df=spark.read
    .format("csv")
    .option("Header",true)
    .schema(schema)
    .option("path","C:/Users/divya/Downloads/Complex_Q2.csv")
    .load()

  df.show()

  df.select(col("review_id"),col("Rating"),when(col("Rating")<3,"Bad")
  .when(col("Rating")===3 || col("Rating")===4,"Good")
  .when(col("Rating")=== 5,"Excellent").alias("feedback"),when(col("Rating")>=3,"true").otherwise("false").alias("Is_positive")).show()
}

}
