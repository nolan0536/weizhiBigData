package zhangmingxuan.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_Transform_test {
  def main(args: Array[String]): Unit = {

    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    //TODO算子 - filter 符合筛选规则的保留，不符合的剔除
    val rdd: RDD[String] = sc.textFile("datas/apache.log")
    rdd.filter(
      line=>{
        val data=line.split(" ")
        val time =data(3)
        time.startsWith("17/05/2015")
      }
    ).collect().foreach(println)
    sc.stop()
  }

}
