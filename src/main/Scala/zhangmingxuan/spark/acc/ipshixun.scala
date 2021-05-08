package zhangmingxuan.spark.acc

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.rdd.RDD
object ipshixun {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf()
      .setAppName("ip")
      .setMaster("local[*]")
    val sc=new SparkContext(sparkconf)
    val rdd: RDD[String] = sc.textFile("file:///D:\\spark学习\\spark-04-Spark案例讲解\\课件与代码\\ip\\access.log")
    val maprdd: RDD[(String, Int)] = rdd.map(
      l => {
        val datas: Array[String] = l.split("[|]")
        (datas(1), 1)
      }
    )
    val reducerdd: RDD[(String, Int)] = maprdd.reduceByKey(_ + _)
    val data1: Array[(String, Int)] = reducerdd.sortBy(x => x._2, false).take(10)
    data1.foreach(println)


    sc.stop()
  }

}
