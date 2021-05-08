package zhangmingxuan.spark.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File1_Par2 {
  def main(args: Array[String]): Unit = {
    //准备环境
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("rdd")
    val sc= new SparkContext(sparkconf)

    //创建rdd
    //从文件中创建rdd，将文件中的数据作为处理的数据源
    // 14byte/2=7byte
    // 14/7=2分区

    //如果数据源为多个文件，那么计算分区时以文件为单位进行分区
    val rdd=sc.textFile("datas/word",2)
    rdd.collect().foreach(println)
    //将处理完的数据保存为分区文件
    rdd.saveAsTextFile("output1")



    //关闭环境
    sc.stop()
  }

}
