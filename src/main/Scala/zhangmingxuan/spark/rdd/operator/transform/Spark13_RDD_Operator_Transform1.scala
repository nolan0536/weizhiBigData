package zhangmingxuan.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {

    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    //TODO算子 - 双value类型

    //交集、并集、差集要求两个数据源数据类型保持一致
    //拉链操作两个数据源的类型可以不一致
    val rdd1 = sc.makeRDD(List(1,2,3,4,5,6),2)
    val rdd2 = sc.makeRDD(List(3,4,5,6),2)


    //拉链
    //两个数据源要求分区数量要保持一致
    //每一个分区元素数量相同时才能进行拉链操作
    val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
    println(rdd6.collect().mkString(","))
    sc.stop()
  }

}
