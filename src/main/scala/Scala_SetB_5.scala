import org.apache.spark.SparkContext

object Scala_SetB_5 {
  def main (args:Array[String]):Unit = {
    val sc = new SparkContext("local[*]","Hema")
    val rdd1=sc.textFile("C:/Users/divya/Downloads/Scala_B_5.txt")
    val rdd2=rdd1.map(x=>x.split(","))
    val rdd3=rdd2.map(x=> (x(1),x(3).toFloat))
    val rdd4=rdd3.reduceByKey((x,y)=>x+y)
    var rdd5=rdd4.sortBy(x=>x._2,false)
    rdd5.take(5).foreach(println)
  }

}
