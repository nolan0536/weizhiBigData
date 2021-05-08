package zhangmingxuan.spark.req

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.rdd.RDD
object SHIXUN1 {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("shixun2")
    val sc=new SparkContext(sparkconf)
    val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    val clickrdd: RDD[String] = rdd.filter(
      action => {
        val datas: Array[String] = action.split("_")
        datas(6) != "-1"
      }
    )
    val clickcont: RDD[(String, Int)] = clickrdd.map(
      action => {
        val datas: Array[String] = action.split("_")
        (datas(6), 1)
      }
    )
    val dianjishu: RDD[(String, Int)] = clickcont.reduceByKey(_ + _)
    val orderrdd: RDD[String] = rdd.filter(
      action => {
        val datas: Array[String] = action.split("_")
        datas(8) != "null"
      }
    )
    val ordercount: RDD[(String, Int)] = orderrdd.flatMap(
      action => {
        val datas: Array[String] = action.split("_")
        val cid = datas(8)
        val ids: Array[String] = cid.split(",")
        ids.map(id => (id, 1))
      }
    )
    val xiadanshu: RDD[(String, Int)] = ordercount.reduceByKey(_ + _)
    val payrdd: RDD[String] = rdd.filter(
      action => {
        val datas: Array[String] = action.split("_")
        datas(10) != "null"
      }
    )
    val paycount: RDD[(String, Int)] = payrdd.flatMap(
      action => {
        val datas: Array[String] = action.split("_")
        val ids= datas(10).split(",")
//        val ids1: Array[String] = ids.split(",")
        ids.map(x => (x, 1))
      }
    )
    val zhifushu: RDD[(String, Int)] = paycount.reduceByKey(_ + _)
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
        (x, (0, 0, y))
      }
    }
    val rdd4: RDD[(String, (Int, Int, Int))] = rdd1.union(rdd2).union(rdd3)
    val rdd5: RDD[(String, (Int, Int, Int))] = rdd4.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )
    val rdd6: Array[(String, (Int, Int, Int))] = rdd5.sortBy(x => x._2, false).take(10)
    rdd6.foreach(println)
    sc.stop()





  }

}
