import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Complex_Q1 {
  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder
      .appName("Spark-program")
      .master("local[*]")
      .getOrCreate()

    val schema=" employee_id Int,age Int,Salary Int"

    val df=spark.read
      .format("csv")
      .option("Header",true)
      .schema(schema)
      .option("path","C:/Users/divya/Downloads/Complex_Q1.csv")
      .load()

    df.show()

    df.select(col("employee_id"),col("age"),col("Salary"),when(col("age")<30 && col("Salary")<35000,"Young & Low Salary")
      .when((col("age")>=30 && col("age")<=40) &&(col("salary")>=35000 && col("salary")<=45000),"Middle Aged & Medium Salary")
    .otherwise("Old & High Salary").alias("category")).show(false)
  }

}
