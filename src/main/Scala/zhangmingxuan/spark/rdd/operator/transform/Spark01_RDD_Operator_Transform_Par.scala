package zhangmingxuan.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform_Par {
  def main(args: Array[String]): Unit = {

    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    //TODO算子 map

    //1.rdd的计算是一个分区内的数据是一个一个执行逻辑
    //  只有前面的一个数据全部的逻辑执行完毕后，才会执行下一个数据
    //  分区内数据的执行是有序的
    val rdd=sc.makeRDD(List(1,2,3,4),2)
    val maprdd: RDD[Int] = rdd.map(
      num => {
        println("num=" + num)
        num * 2
      }
    )
    val maprdd1: RDD[Int] = maprdd.map(
      num => {
        println("num++++=" + num)
        num * 2
      }
    )
    maprdd1.collect()
    sc.stop()
  }

}
