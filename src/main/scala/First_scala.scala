import org.apache.spark.SparkContext

object First_scala {
  def main(args:Array[String]):Unit={

    val sc=new SparkContext("local[*]","Hema")
    val rdd1=sc.textFile("C:/Users/divya/Downloads/Hema.txt")
    val rdd2=rdd1.flatMap(x=>x.split("/"))
    val rdd3=rdd2.map(x=>(x,1))
    val rdd4=rdd3.reduceByKey((x,y)=>x+y)
    val rdd5=rdd4.sortBy(x=>x._2,false)
    rdd5.take(1).foreach(println)

  }

}