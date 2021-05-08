package zhangmingxuan.spark.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext,SparkConf}
object xuqiu1 {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf()
      .setAppName("xuqiu1")
      .setMaster("local[*]")
    val sc=new SparkContext(sparkconf)
    val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    val flatrdd: RDD[(String, (Int, Int, Int))] = rdd.flatMap {
      action => {
        val datas: Array[String] = action.split("_")
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
    val reducerdd: RDD[(String, (Int, Int, Int))] = flatrdd.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )
    reducerdd.sortBy(x=>x._2,false).take(10).foreach(println)

    sc.stop()
  }

}
