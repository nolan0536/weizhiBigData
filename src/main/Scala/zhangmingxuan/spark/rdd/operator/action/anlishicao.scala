package zhangmingxuan.spark.rdd.operator.action

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.rdd.RDD
object anlishicao {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("shicao")
    val sc=new SparkContext(sparkconf)
    val rdd: RDD[String] = sc.textFile("datas/agent.log")
    val rdd1: RDD[((String, String), Int)] = rdd.map(
      line => {
        val datas = line.split(" ")
        ((datas(1), datas(4)), 1)
      }
    )
    val rdd2: RDD[((String, String), Int)] = rdd1.reduceByKey(_ + _)
    val rdd3: RDD[(String, (String, Int))] = rdd2.map {
      case ((x, y), z) => {
        (x, (y, z))
      }
    }
    val rdd4: RDD[(String, Iterable[(String, Int)])] = rdd3.groupByKey()
    val rdd5: RDD[(String, List[(String, Int)])] = rdd4.mapValues(
      iter => {
        iter.toList.sortBy(x => x._2)(Ordering.Int.reverse).take(3)
      }
    )
    rdd5.collect().foreach(println)


  }

}
