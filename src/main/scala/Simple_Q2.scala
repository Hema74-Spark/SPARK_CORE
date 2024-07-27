import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object Simple_Q2 {

  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder()
      .appName("Spark-Program")
      .master("local[*]")
      .getOrCreate()
    val schema=StructType(List(
      StructField("Student_id",IntegerType),
      StructField("score",IntegerType)
    ))
    val df=spark.read
      .format("csv")
      .option("Header",true)
      .schema(schema)
      .option("path","C:/Users/divya/Downloads/Simple_Q2.csv")
      .load()
    df.show(5,false)

    df.select(col("Student_id"),col("score"),
    when(col("score")>=50,"Pass").otherwise("Fail").alias("Grade")).show()

  }

}
