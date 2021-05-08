package zhangmingxuan.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    //TODO算子 - coalesce方法默认情况下不会将分区的数据打乱重新组合
    //这种情况下的缩减分区可能导致数据不均衡
    //如果想要让数据均衡，可以进行shuffle处理
    //使用第二个参数
    //扩大分区：repartition,底层代码调用的就是coalesce，而且肯定采用shuffle
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4,5,6),2)
    //coalesce算子可以扩大分区的，但是如果不进行shuffle操作，是没有意义的
    val rdd1: RDD[Int] = rdd.coalesce(3,true)
    val rdd2: RDD[Int] = rdd.repartition(3)
    rdd2.saveAsTextFile("outpath")
    sc.stop()
  }

}
