package zhangmingxuan.spark.rdd.builder

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.rdd.RDD
object Spark01_RDD_Memory {
  def main(args: Array[String]): Unit = {
    //准备环境
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("rdd")
    val sc= new SparkContext(sparkconf)

    //创建rdd
    //从内存中创建rdd，将内存中集合的数据作为处理的数据源
    val seq=Seq[Int](1,2,3,4)
    //parallelize:表示的是并行
    val rdd:RDD[Int]=sc.makeRDD(seq)
//    val rdd:RDD[Int]= sc.parallelize(seq)
    rdd.collect().foreach(println)


    //关闭环境
    sc.stop()
  }

}
