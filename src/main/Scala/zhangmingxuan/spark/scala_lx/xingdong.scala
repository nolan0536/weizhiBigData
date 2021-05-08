package zhangmingxuan.spark.scala_lx

import org.apache.spark.{SparkConf, SparkContext}
object xingdong {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf()
      .setMaster("local[2]")
      .setAppName("suanzi")
    val sc= new SparkContext(sparkConf)
    val arrRDD=sc.parallelize(Array(1,2,3,4,5,67,11))
    println(arrRDD.reduce((a,b)=>a+b))
    println(arrRDD.collect())
    arrRDD.foreach(x=>println(x))
    println(arrRDD.countByValue())


  }

}
