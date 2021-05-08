package zhangmingxuan.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark21_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    //TODO算子 - key——value类型
    //  join :两个不同数据源的数据，相同的key和value会连接在一起，形成元组
    //  如果两个数据源中的key没有匹配上，那么数据不会出现在结果中
    //  如果两个数据源中key有多个相同的，会依次匹配，可能会出现笛卡尔乘积，数据会出现几何增长

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("b", 1), ("c", 3)
    ))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 4), ("b", 5), ("a", 6)
    ))
    val rdd3: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    rdd3.collect().foreach(println)
    sc.stop() 
  }

}
