package zhangmingxuan.spark.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Req2_HotCategoryTop10SessionAnalysis {
  def main(args: Array[String]): Unit = {
    //TODO:top10热门品类
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("shixun")
    val sc=new SparkContext(sparkconf)

    val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    rdd.cache()
    val top10ids: Array[String] = top10Category(rdd)
    top10ids.foreach(println)
    //1.过滤原始数据，保留点击和前十品类id
    val filterrdd: RDD[String] = rdd.filter(
      action => {
        val datas: Array[String] = action.split("_")
        if (datas(6) != "-1") {
          top10ids.contains(datas(6))
        } else {
          false
        }
      }
    )
    //2.根据品类id和sessionid进行点击量的统计
    val reducerdd: RDD[((String, String), Int)] = filterrdd.map(
      action => {
        val datas: Array[String] = action.split("_")
        ((datas(6), datas(2)), 1)
      }
    ).reduceByKey(_ + _)
    //3.将统计的结果进行结构的转换
    //（（品类id，sessionID），sum）=》（品类id，（sessionid，sum））
    val map_rdd: RDD[(String, (String, Int))] = reducerdd.map {
      case ((cid, sid), sum) => {
        (cid, (sid, sum))
      }
    }
    //4.相同的品类分在同一组中
    val grouprdd: RDD[(String, Iterable[(String, Int)])] = map_rdd.groupByKey()
    //5.将分组后的数据进行点击量排序取前十名
    val rdd5: RDD[(String, List[(String, Int)])] = grouprdd.mapValues {
      iter => {
        iter.toList.sortBy(l => l._2)(Ordering.Int.reverse).take(10)
      }
    }
    rdd5.collect().foreach(println)
    sc.stop()
  }
  def top10Category(rdd:RDD[String]) ={
    val flatrdd: RDD[(String,(Int,Int,Int))] = rdd.flatMap(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          val ids = datas(8).split(",")
          ids.map(id => (id, (0, 1, 0)))
        } else if (datas(10) != "null") {
          val ids = datas(10).split(",")
          ids.map(id => (id, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )

    val analysisRDD: RDD[(String, (Int, Int, Int))] = flatrdd.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    analysisRDD.sortBy(x => x._2, false).take(10).map(_._1)

  }
}
