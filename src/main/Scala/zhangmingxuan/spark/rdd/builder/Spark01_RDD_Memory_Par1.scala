package zhangmingxuan.spark.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory_Par1 {
  def main(args: Array[String]): Unit = {
    //准备环境
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("rdd")
    val sc=new SparkContext(sparkconf)

    //创建rdd
    val seq = Seq(1,2,3,4,5)
    val rdd: RDD[Int] = sc.makeRDD(seq,3 )
    //将处理的数据保存成分区文件
    rdd.saveAsTextFile("output")

    //关闭环境
    sc.stop()

  }

}
