package zhangmingxuan.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    //TODO算子 - key——value类型

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("a", 4)
    ),2)
    //aggregateByKey存在函数柯里化，有两个参数列表
    //第一个参数列表 需要传递一个参数，表示为初始值
    //           主要用于当碰见第一个key的时候，和value进行分区内计算
    //第二个参数列表需要传递两个参数
    //           第一个参数表示分区内计算规则
    //           第二个参数表示分区间计算规则
    val rdd1: RDD[(String, Int)] = rdd.aggregateByKey(0)( //（a，0）
      //先取出两个数中的最大值
      (x, y) => math.max(x, y),
      //再把两个数相加
      (x, y) => x + y
    )
    rdd1.collect().foreach(println)
    sc.stop()
  }

}
