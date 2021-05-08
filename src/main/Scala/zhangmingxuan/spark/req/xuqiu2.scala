package zhangmingxuan.spark.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext,SparkConf}
object xuqiu2 {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf()
      .setAppName("xuqiu1")
      .setMaster("local[*]")
    val sc=new SparkContext(sparkconf)
    val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    val flatmap10: Array[String] = flatmap(rdd)
    val fiterrdd: RDD[String] = rdd.filter(
      action => {
        val datas: Array[String] = action.split("_")
        if (datas(6) != "-1") {
          flatmap10.contains(datas(6))
        } else {
          false
        }
      }
    )
    val maprdd: RDD[((String, String), Int)] = fiterrdd.map {
      action => {
        val datas: Array[String] = action.split("_")
        ((datas(6), datas(2)), 1)
      }
    }.reduceByKey(_ + _)
    val rdd2: RDD[(String, (String, Int))] = maprdd.map {
      case ((x, y), sum) => {
        (x, (y, sum))
      }
    }
    val grouprdd: RDD[(String, Iterable[(String, Int)])] = rdd2.groupByKey()
    grouprdd.mapValues(
      iter=>{
        iter.toList.sortBy(x=>x._2)(Ordering.Int.reverse).take(10)
      }
    ).collect().foreach(println)
  }
  def flatmap(rdd:RDD[String]) ={
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
    val takerdd: Array[(String, (Int, Int, Int))] = reducerdd.sortBy(x => x._2, false).take(10)
    takerdd.map(l=>l._1)

  }
}
