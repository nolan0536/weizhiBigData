package zhangmingxuan.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    //TODO算子 - groupby
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    //groupby会将数据源中的每一个数据进行分组判断，根据返回的分组key进行分组
    //相同的key值的数据会放置在一个组中
    val grouprdd: RDD[(Int, Iterable[Int])] = rdd.groupBy(x => x % 2)
    grouprdd.collect().foreach(println)
    sc.stop()
  }

}
