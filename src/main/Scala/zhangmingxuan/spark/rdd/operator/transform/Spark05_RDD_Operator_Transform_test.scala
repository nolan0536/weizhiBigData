package zhangmingxuan.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Transform_test {
  def main(args: Array[String]): Unit = {

    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    //TODO算子 - glom
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    //把数据直接转换为相同类型的数组
    val glomrdd: RDD[Array[Int]] = rdd.glom()
    //先求出分区中的最大值在求和
    val maxrdd: RDD[Int] = glomrdd.map(
      array => {
        array.max
      }
    )
    println(maxrdd.collect().sum)
    sc.stop()
  }

}
