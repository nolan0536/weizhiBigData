package zhangmingxuan.spark.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File1_Par1 {
  def main(args: Array[String]): Unit = {
    //准备环境
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("rdd")
    val sc= new SparkContext(sparkconf)

    //创建rdd
    //从文件中创建rdd，将文件中的数据作为处理的数据源
    //TODO 数据分区的分配
    //1.数据以行为单位进行读取
    //    spark读取文件，采用的是hadoop的方式读取，所以一行一行读取，和字节数没有关系
    //2.数据读取时以偏移量为单位,偏移量不会被重复读取
    /*
    1@@ =》 012
    2@@ =》 345
    3   =》 6
     */
    //3.数据分区的偏移量范围的计算
    //0=》[0,3] =》12
    //1=>[3,6] =》3
    //2=>[6,7]
    val rdd=sc.textFile("datas/1.txt",2)
    rdd.collect().foreach(println)
    //将处理完的数据保存为分区文件
    rdd.saveAsTextFile("output1")



    //关闭环境
    sc.stop()
  }

}
