package zhangmingxuan.spark.req

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.rdd.RDD
object shixun5 {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("qingxi2")
    val sc=new SparkContext(sparkconf)
    val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    val click: RDD[String] = rdd.filter(
      action => {
        val datas: Array[String] = action.split("_")
        datas(6) != "-1"
      }
    )
    val dianjishu: RDD[(String, Int)] = click.map(
      action => {
        val datas: Array[String] = action.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)
    val order: RDD[String] = rdd.filter(
      action => {
        val datas: Array[String] = action.split("_")
        datas(8) != "null"
      }
    )
    val xiadanshu: RDD[(String, Int)] = order.flatMap(
      action => {
        val datas: Array[String] = action.split("_")
        val ids: Array[String] = datas(8).split(",")
        ids.map(l => (l, 1))
      }
    ).reduceByKey(_+_)
    val pay: RDD[String] = rdd.filter(
      action => {
        val datas: Array[String] = action.split("_")
        datas(10) != "null"
      }
    )
    val zhifushu: RDD[(String, Int)] = pay.flatMap(
      action => {
        val datas: Array[String] = action.split("_")
        val ids: Array[String] = datas(10).split(",")
        ids.map(l => (l, 1))
      }
    ).reduceByKey(_+_)
    val rdd1: RDD[(String, (Int, Int, Int))] = dianjishu.map {
      case (x, y) => {
        (x, (y, 0, 0))
      }
    }
    val rdd2: RDD[(String, (Int, Int, Int))] = xiadanshu.map {
      case (x, y) => {
        (x, (0, y, 0))
      }
    }
    val rdd3: RDD[(String, (Int, Int, Int))] = zhifushu.map {
      case (x, y) => {
        (x, (0, 0, 1))
      }
    }
    val rdd4: RDD[(String, (Int, Int, Int))] = rdd1.union(rdd2).union(rdd3)
    val rdd5: RDD[(String, (Int, Int, Int))] = rdd4.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )
    rdd5.sortBy(x=>x._2,false).take(10).foreach(println)
    sc.stop()
  }

}
