import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Complex_Q3 {

  def main(args:Array[String]): Unit = {
    val spark=SparkSession.builder
      .appName("Saprk-Program")
      .master("local[*]")
      .getOrCreate()

    val schema= "doc_id Int,content String"

    val df=spark.read
      .format("csv")
      .option("Header",true)
      .schema(schema)
      .option("path","C:/Users/divya/Downloads/Complex_Q3.csv")
      .load()
    df.show(false)

    df.select(col("doc_id"),col("content"),when(col("content").contains("fox"),"Animal Related")
    .when(col("content").contains("Lorem"),"Placeholder Text")
    .when(col("content").contains("Spark"),"Tech Related").alias("content_category")).show(false)

  }

}
