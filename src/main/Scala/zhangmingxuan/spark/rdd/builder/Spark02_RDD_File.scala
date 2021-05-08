package zhangmingxuan.spark.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {
    //准备环境
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("rdd")
    val sc= new SparkContext(sparkconf)

    //创建rdd
    //从文件中创建rdd，将文件中的数据作为处理的数据源
    //path路径默认以当前环境的根路径为基准，可以写绝对路径，也可以写相对路径
    //path路径可以是文件的具体路径，也可以是目录名称
    //path路径还可以使用通配符*
    //path还可以是分布式存储系统路径：hdfs
    val rdd: RDD[String] = sc.textFile("datas")
    rdd.collect().foreach(println)



    //关闭环境
    sc.stop()
  }

}
