package zhangmingxuan.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    //flatMap
    val rdd: RDD[List[Int]] = sc.makeRDD(List(
      List(1, 2),List(3, 4)))
    val rddflat: RDD[Int] = rdd.flatMap(
      list =>{
        list
      }
    )


    rddflat.collect().foreach(println)
    sc.stop()
  }

}
