import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object Simple_Q3 {

  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder()
      .appName("Spark-Program")
      .master("local[*]")
      .getOrCreate()
    val schema=StructType(List(
      StructField("transaction_id",IntegerType),
      StructField("amount",IntegerType)
    ))

    val df=spark.read
      .format("csv")
      .option("Header",true)
      .schema(schema)
      .option("path","C:/Users/divya/Downloads/Simple_Q3.csv")
      .load()
    df.show(5,false)
    df.select(col("transaction_id"),col("amount"),
      when(col("amount")>1000,"High").when(col("amount")>=500 && col("amount")<=1000,"Medium").otherwise("Low").alias("Category"))
      .show()
  }

}
