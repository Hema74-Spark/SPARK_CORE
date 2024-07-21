import org.apache.spark.SparkContext

object Scala_SetD_5 {
  def main(args:Array[String]):Unit={
    val sc = new SparkContext("local[*]","Hema")
    val rdd1=sc.textFile("C:/Users/divya/Downloads/Scala_D_5.txt")
    val rdd2 = rdd1.flatMap(x => x.split(","))
    val rdd3=rdd2.flatMap(x=> x.split(" "))
    val rdd4=rdd3.map(x=> (x,1))
    val rdd5=rdd4.reduceByKey((x,y) => x+y)
    val rdd6=rdd5.sortBy(x=>x._2,false)
    rdd5.take(5).foreach(println)
  }

}
