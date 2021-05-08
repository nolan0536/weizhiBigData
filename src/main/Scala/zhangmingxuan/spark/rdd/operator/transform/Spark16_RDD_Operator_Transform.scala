package zhangmingxuan.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark16_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    //TODO算子 - key——value类型

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("b", 4)
    ))
    //groupbykey:将数据源中的数据，根据key的数据分在一个组中，形成一个对偶元组
    //           元组中的第一个元素就是key
    //           元组中的第二个元素就是相同key的value集合
    val grouprdd: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    val groupby1: RDD[(String, Int)] = grouprdd.map(
      line => {
        (line._1, line._2.sum)
      }
    )
    grouprdd.collect().foreach(println)
    groupby1.collect().foreach(println)
    val rdd2: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(x => x._1)
//    rdd2.collect().foreach(println)
    sc.stop()
  }

}
