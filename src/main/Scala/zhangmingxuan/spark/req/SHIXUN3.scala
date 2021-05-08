package zhangmingxuan.spark.req

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.rdd.RDD
object SHIXUN3 {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("shixun3")
    val sc = new SparkContext(sparkconf)
    val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    val dianji: RDD[String] = rdd.filter(
      x => {
        val datas: Array[String] = x.split("_")
        datas(6) != "-1"
      }
    )
    val dianjishu: RDD[(String, Int)] = dianji.map(
      x => {
        val datas: Array[String] = x.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)
    dianjishu.take(10).foreach(println)
    val xiadan: RDD[String] = rdd.filter(
      x => {
        val datas: Array[String] = x.split("_")
        datas(8) != "null"
      }
    )
    val xiadanshu: RDD[(String, Int)] = xiadan.flatMap(
      x => {
        val datas: Array[String] = x.split("_")
        val ids: Array[String] = datas(8).split(",")
        ids.map(x => (x, 1))
      }
    ).reduceByKey(_+_)
    val zhifu: RDD[String] = rdd.filter(
      x => {
        val datas: Array[String] = x.split("_")
        datas(10) != "null"
      }
    )
    val zhifushu: RDD[(String, Int)] = zhifu.flatMap(
      x => {
        val datas: Array[String] = x.split("_")
        val ids: Array[String] = datas(10).split(",")
        ids.map(x => (x, 1))
      }
    ).reduceByKey(_ + _)
    val rdd4: RDD[(String, (Int, Int, Int))] = dianjishu.map {
      case (x, y) => {
        (x, (y, 0, 0))
      }
    }
    val rdd5: RDD[(String, (Int, Int, Int))] = xiadanshu.map {
      case (x, y) => {
        (x, (0, y, 0))
      }
    }
    val rdd6: RDD[(String, (Int, Int, Int))] = zhifushu.map {
      case (x, y) => {
        (x, (0, 0, y))
      }
    }
    val rdd7: RDD[(String, (Int, Int, Int))] = rdd4.union(rdd5).union(rdd6)
    val rdd8: RDD[(String, (Int, Int, Int))] = rdd7.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )
    val rdd9: Array[(String, (Int, Int, Int))] = rdd8.sortBy(x => x._2, false).take(10)
    rdd9.foreach(println)
    sc.stop()

  }

}
