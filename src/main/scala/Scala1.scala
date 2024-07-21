import org.apache.spark.SparkContext

object Scala1 {
  def main(args:Array[String]):Unit={
    val sc=new SparkContext("local[*]","Hema")
    val rdd1=sc.textFile("C:/Users/divya/Downloads/Scala_1.txt")
    val rdd2=rdd1.map(x=>x.split(","))
    val rdd3=rdd2.map(x=>(x(1),x(3).toFloat))
    val rdd4=rdd3.reduceByKey((x,y)=>x+y)
    rdd4.collect.foreach(println)
  }

}
