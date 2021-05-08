package zhangmingxuan.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    //TODO算子 - filter 符合筛选规则的保留，不符合的剔除
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //只保留奇数
    val filterRDD: RDD[Int] = rdd.filter(x => x % 2 != 0)
    filterRDD.collect().foreach(println)
    sc.stop()
  }

}
