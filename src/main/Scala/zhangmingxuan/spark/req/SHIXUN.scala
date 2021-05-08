package zhangmingxuan.spark.req

import java.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object SHIXUN {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("shixun")
    val sc=new SparkContext(sparkconf)
    val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    val rdd2: RDD[(String,(Int,Int,Int))] = rdd.flatMap{
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          val ids: Array[String] = datas(8).split(",")
          ids.map(id => (id, (0, 1, 0)))
        } else if (datas(10) != "null") {
          val ids: Array[String] = datas(10).split(",")
          ids.map(id => (id, (0, 0, 1)))
        } else {
          Nil
        }
      }
    }
//    rdd2.take(10).foreach(println)
    val rdd3: RDD[(String, (Int, Int, Int))] = rdd2.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )
//    rdd3.take(10).foreach(println)
    val rdd4: Array[(String, (Int, Int, Int))] = rdd3.sortBy(x => x._2, false).take(10)
    rdd4.foreach(println)

    sc.stop()
  }

}
