package zhangmingxuan.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark24_RDD_Req {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    //案例实操
    //1.获取原始数据
    val rdd: RDD[String] = sc.textFile("datas/agent.log")
    //2.将原始数据根据结构的转换，方便统计
    val rdd1: RDD[Array[String]] = rdd.map(
      x => {
        x.split(" ")
      }
    )
    val rdd2: RDD[((String, String), Int)] = rdd1.map(
      l => {
        ((l(1), l(4)), 1)
      }
    )
    //3.将转换结构的数据，进行分组聚合
    val rdd3: RDD[((String, String), Int)] = rdd2.reduceByKey((x: Int, y: Int) => x + y)
    //4.将聚合的结构进行结构的转换
    val rdd4: RDD[(String, (String, Int))] = rdd3.map {
      case ((prv, ad), sum) => {
        (prv, (ad, sum))
      }
    }
    //5.将转换结构后的数据根据省份进行分组
    val rdd5: RDD[(String, Iterable[(String, Int)])] = rdd4.groupByKey()
    //6.将分组后的数据组内排序（降序），取前三名
    val resultRDD: RDD[(String, List[(String, Int)])] = rdd5.mapValues(
      iter => {
        //想转化成可以进行排序操作的toList
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )
    resultRDD.collect().foreach(println)
    sc.stop()
  }
}
