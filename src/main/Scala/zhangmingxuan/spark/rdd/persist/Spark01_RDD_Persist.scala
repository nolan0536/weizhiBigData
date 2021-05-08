package zhangmingxuan.spark.rdd.persist

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.rdd.RDD
object Spark01_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    val list =List("hello scala","hello spark")
    val rdd: RDD[String] = sc.makeRDD(list)
    val flatrdd: RDD[String] = rdd.flatMap(x => x.split(" "))
    val maprdd: RDD[(String, Int)] = flatrdd.map(
      x => {
        println("*word")
        (x, 1)
    })
    val reducerdd: RDD[(String, Int)] = maprdd.reduceByKey(_ + _)
    reducerdd.collect().foreach(println)
    println("***********************************")
    val grouprdd: RDD[(String, Iterable[Int])] = maprdd.groupByKey()
    grouprdd.collect().foreach(println)

    sc.stop()

  }

}
