import org.apache.spark.SparkContext
object Scala_SetA_3 {
  def main(args:Array[String]):Unit ={
    val sc= new SparkContext("local[*]","Hema")
    val rdd1 = sc.textFile("C:/Users/divya/Downloads/Scala_3.txt")
    val rdd2 = rdd1.map(x => x.split(","))
    val rdd3 = rdd2.map(x=> (x(0),x(2).toFloat))
    val rdd4=rdd3.map{case(x,y) => (x,(y,1))}
    val rdd5=rdd4.reduceByKey{case((x,y),(a,b))=> (x+a,y+b)}
    val rdd6=rdd5.map{case(x,(a,b))=>(x,a/b)}

    rdd6.collect.foreach(println)


  }

}
