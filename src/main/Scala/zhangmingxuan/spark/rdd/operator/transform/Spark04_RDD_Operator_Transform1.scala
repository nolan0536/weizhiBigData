package zhangmingxuan.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {

    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    //flatMap
    val rdd: RDD[String] = sc.makeRDD(List(
      "hell scala", "hell spark"))
    val rddflat: RDD[String] = rdd.flatMap(line => line.split(" "))
    val rddmap: RDD[(String, Int)] = rddflat.map(x => (x, 1))

    val rddgroup: RDD[((String, Int), Iterable[(String, Int)])] = rddmap.groupBy(x => x)
    rddgroup.collect().foreach(println)
    sc.stop()
  }

}
