package zhangmingxuan.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform_test {
  def main(args: Array[String]): Unit = {

    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    //TODO算子 map
    val rdd: RDD[String] = sc.textFile("datas/apache.log")

    //长的字符串转换成短的字符串
    //从服务器日志数据apache.log中获取用户请求url资源路径
    val rdd1: RDD[String] = rdd.map(
      line => {
        val datas=line.split(" ")
        datas(6)
      }
    )
    rdd1.collect().foreach(println)

    sc.stop()
  }

}
