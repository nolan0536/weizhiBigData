package zhangmingxuan.spark.rdd.operator.transform

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.rdd.RDD
object lianxi {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("lianxi")
    val sc=new SparkContext(sparkconf)
    val rdd: RDD[String] = sc.makeRDD(List("hello spark", "hello list"))
    val rddmap: RDD[String] = rdd.flatMap(line => line.split(" "))
    rddmap.collect().foreach(println)
    sc.stop()
  }

}
