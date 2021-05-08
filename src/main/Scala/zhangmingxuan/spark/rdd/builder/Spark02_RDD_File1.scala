package zhangmingxuan.spark.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File1 {
  def main(args: Array[String]): Unit = {
    //准备环境
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("rdd")
    val sc= new SparkContext(sparkconf)

    //创建rdd
    //从文件中创建rdd，将文件中的数据作为处理的数据源
    //textFile：以行为单位来读取数据
    //wholeTextFiles：以文件为单位读取数据
    //读取的结果表示为元组，第一个元素表示文件路径，第二个元素表示文件内容
    val rdd=sc.wholeTextFiles("datas")
    rdd.collect().foreach(println)



    //关闭环境
    sc.stop()
  }

}
