package zhangmingxuan.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform2 {
  def main(args: Array[String]): Unit = {

    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    //flatMap
    val rdd: RDD[Any] = sc.makeRDD(List(
      List(1, 2), List(3, 4), 5))
    val rddflat: RDD[Any] = rdd.flatMap(
      data => {
        data match {
          case list: List[_] => list
          case dat => List(dat)
        }
      }
    )


    rddflat.collect().foreach(println)
    sc.stop()
  }

}
