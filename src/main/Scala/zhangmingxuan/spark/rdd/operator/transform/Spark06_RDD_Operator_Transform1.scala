package zhangmingxuan.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {

    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    //TODO算子 - groupby
    val rdd: RDD[String] = sc.makeRDD(List("hello", "spark", "scala", "hadoop"), 2)
    //获取到各字符串的首字母，按照首字母进行分组
    val grouprdd: RDD[(Char, Iterable[String])] = rdd.groupBy(x => x.charAt(0))
    grouprdd.collect().foreach(println)
    grouprdd.saveAsTextFile("output2")
    sc.stop()
  }

}
