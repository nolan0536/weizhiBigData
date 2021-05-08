package zhangmingxuan.spark.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("Action")
    val sc=new SparkContext(sf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)

    //TODO -行动算子
    //不需要考虑数据的类型
    //aggregateBYKey:初始值只会参与分区内计算
    //aggregate：初始值会参与分区内计算，并且和参与分区间计算
    val rdd2: Int = rdd.aggregate(10)(_ + _, _ + _)
    println(rdd2)
    val rdd3: Int = rdd.fold(10)(_ + _)
    println(rdd3)







    sc.stop()

  }
}
