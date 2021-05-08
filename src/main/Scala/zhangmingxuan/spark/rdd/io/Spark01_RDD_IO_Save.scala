package zhangmingxuan.spark.rdd.io

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
object Spark01_RDD_IO_Save {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1),
      ("b", 2),
      ("c", 3)
    ))
    rdd.saveAsTextFile("output1")
    rdd.saveAsObjectFile("output2")
    rdd.saveAsSequenceFile("output3")
    sc.stop()
  }

}
