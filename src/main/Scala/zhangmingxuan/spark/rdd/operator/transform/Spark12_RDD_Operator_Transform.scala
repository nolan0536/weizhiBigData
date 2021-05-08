package zhangmingxuan.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    //TODO算子 - sortBy
    val rdd: RDD[Int] = sc.makeRDD(List(1, 4, 5, 8, 0, 9), 2)
    val newRDD: RDD[Int] = rdd.sortBy(num => num)
    newRDD.saveAsTextFile("outpath")
    sc.stop()
  }

}
