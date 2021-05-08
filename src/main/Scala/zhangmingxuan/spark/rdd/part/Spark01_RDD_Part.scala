package zhangmingxuan.spark.rdd.part

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
object Spark01_RDD_Part {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    val rdd: RDD[(String, String)] = sc.makeRDD(List(
      ("nba", "************"),
      ("cba", "************"),
      ("wnba", "************"),
      ("nba", "************")
    ), 3)
    val partRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)
    partRDD.saveAsTextFile("output")
    sc.stop()
  }

  //自定义分区器：
  //1.继承Partitioner类
  //2.重写方法
  class MyPartitioner extends Partitioner {
    //分区数量
    override def numPartitions: Int = 3

    //根据数据的key值返回数据所在的分区索引（从0开始）
    override def getPartition(key: Any): Int = {
      key match {
        case "nba"=>0
        case "wnba"=>1
        case _=>2
      }
    }
  }
}
