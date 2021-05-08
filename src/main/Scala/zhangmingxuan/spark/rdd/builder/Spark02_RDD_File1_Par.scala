package zhangmingxuan.spark.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File1_Par {
  def main(args: Array[String]): Unit = {
    //准备环境
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("rdd")
    val sc= new SparkContext(sparkconf)

    //创建rdd
    //从文件中创建rdd，将文件中的数据作为处理的数据源
    //textFile可以将文件作为数据处理的数据源，默认也可以设定分区
    //minPartitions:最小分区数量
    //math.min(defaultParallelism,2)
    //如果不想使用默认的分区数量，可以通过第二个参数指定分区数
    //Spark读取文件，底层其实使用的就是Hadoop的读取方式
    val rdd=sc.textFile("datas/1.txt",2)
    rdd.collect().foreach(println)
    //将处理完的数据保存为分区文件
    rdd.saveAsTextFile("output1")



    //关闭环境
    sc.stop()
  }

}
