import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, month, when}

object Complex_Q9 {

  def main(args:Array[String]):Unit={
    val sparkconf=new SparkConf()
      sparkconf.set("spark.appName","Spark-Program")
    sparkconf.set("spark.master","local[*]")

    val spark=SparkSession.builder
      .config(sparkconf)
      .getOrCreate()

    val schema="payment_id Int,payment_date date"

    val df=spark.read
      .format("csv")
      .option("Header",true)
      .schema(schema)
      .option("path","C:/Users/divya/Downloads/Complex_Q9.csv")
      .load()

    df.show()
    df.select(col("payment_id"),col("payment_date"),when(month(col("payment_date"))=== 1 || month(col("payment_date"))=== 2 ||
      month(col("payment_date"))=== 3,"Q1").when(month(col("payment_date"))=== 4 || month(col("payment_date"))=== 5 ||
      month(col("payment_date"))=== 6,"Q2").when(month(col("payment_date"))=== 7 || month(col("payment_date"))=== 8 ||
      month(col("payment_date"))=== 9,"Q3").otherwise("Q4").alias("quarter")).show()
  }

}
