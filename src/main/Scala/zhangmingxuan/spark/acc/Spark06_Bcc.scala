package zhangmingxuan.spark.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark06_Bcc {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("acc")
    val sc=new SparkContext(sparkconf)
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3)
    ))
    val map2 = mutable.Map(("a",4),("b",5),("c",6))
    //封装广播变量
    val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map2)
    rdd1.map{
      case(w,c)=>{
        //访问广播变量
        val l: Int = bc.value.getOrElse(w, 0)
        (w,(c,l))
      }
    }.collect().foreach(println)
    sc.stop()
  }

}
