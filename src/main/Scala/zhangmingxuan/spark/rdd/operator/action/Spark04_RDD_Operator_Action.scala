package zhangmingxuan.spark.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("Action")
    val sc=new SparkContext(sf)
//    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
val rdd: RDD[(String, Int)] = sc.makeRDD(List(
  ("a", 1), ("b", 2), ("c", 3), ("c", 3)
))

    //TODO -行动算子
    //countBYValue()统计值出现的次数
//    val rdd1: collection.Map[Int, Long] = rdd.countByValue()
//    print(rdd1)
    val stringToL: collection.Map[String, Long] = rdd.countByKey()
    println(stringToL)






    sc.stop()

  }
}
