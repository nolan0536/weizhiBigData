package zhangmingxuan.spark.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark05_Bcc {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("acc")
    val sc=new SparkContext(sparkconf)
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3)
    ))
    val map2 = mutable.Map(("a",4),("b",5),("c",6))
//    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(
//      ("a", 4), ("b", 5), ("c", 6)
//    ))
//    //join会导致数据量的几何增长，并且会影响shuffle的性能，不推荐使用
//    val joinrdd: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
//    //（a,(1,4)）,(b,(2,5)),(c,(3,6))
//    joinrdd.collect().foreach(println)
    rdd1.map{
      case(w,c)=>{
        val l: Int = map2.getOrElse(w, 0)
        (w,(c,l))
      }
    }.collect().foreach(println)
    sc.stop()
  }

}
