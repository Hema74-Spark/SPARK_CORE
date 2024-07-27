import org.apache.spark.sql.functions.when
import org.apache.spark
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Simple_Q1 {
  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder()
      .appName("spark-program")
      .master("local[*]")
      .getOrCreate()
    val schema= StructType(List(
      StructField("Id",IntegerType),
      StructField("Name",StringType),
      StructField("age",IntegerType)
    ))
    val df=spark.read
      .format("csv")
      .option("Header",true)
      .schema(schema)
      .option("path","C:/Users/divya/Downloads/Simple_Q1.csv")
      .load()
    df.show(5,false)

    df.select(col("id"),col("Name"),col("age"),
      when(col("age")>=18, "True").otherwise("False").alias("Is_adult")).show()


  }

}
