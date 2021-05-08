package zhangmingxuan.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    //TODO算子 - sample 根据指定规则抽取数据
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    //sample需要传递三个参数
    //1.第一个参数表示，抽取数据后是否将数据返回true（放回），false（丢弃）
    //2.第二个参数表示，
    //               如果抽取不放回的场合：数据源中的每条数据被抽取的概率 基准值的概念
    //               如果抽取放回的场合：表示数据源中的每条数据被抽取的可能次数
    //3.第三个参数表示，抽取数据时随机算法的种子
    //               如果不传递第三个参数，使用的是当前的系统时间
    val samp: RDD[Int] = rdd.sample(
      true,
      2
//
    )
    println(samp.collect().mkString(","))
    sc.stop()
  }

}
