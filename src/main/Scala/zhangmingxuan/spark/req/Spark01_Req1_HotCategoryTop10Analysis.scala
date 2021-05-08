package zhangmingxuan.spark.req

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.rdd.RDD
object Spark01_Req1_HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    //TODO:top10热门品类
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("shixun")
    val sc=new SparkContext(sparkconf)
    //1.读取原始日志数据
    val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")

    //2.统计品类的点击数量（品类ID，点击数量）
    val clickActionRDD: RDD[String] = rdd.filter(
      action => {
        val datas = action.split("_")
        datas(6) != "-1"
      }
    )
    val clicCountRDD: RDD[(String, Int)] = clickActionRDD.map(
      x => {
        val datas = x.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)
    //3.统计品类的下单数量
    val orderActionRDD: RDD[String] = rdd.filter(
      action => {
        val datas = action.split("_")
        datas(8) != "null"
      }
    )
    //把整体变成个体 /扁平化操作
    val orderCountRDD: RDD[(String, Int)] = orderActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(8)
        val cids = cid.split(",")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)
    //4.统计品类的支付数量
    val payActionRDD: RDD[String] = rdd.filter(
      action => {
        val datas = action.split("_")
        datas(10) != "null"
      }
    )
    //把整体变成个体 /扁平化操作
    val payCountRDD: RDD[(String, Int)] = payActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(10)
        val cids = cid.split(",")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)
    //最重要的是第五步
    //5.将品类进行排序，并且取前10名
    //点击数量排序  下单数量排序 支付数量排序
    //元组排序：先比较第一个，在比较第二个，在比较第三个
    //（品类id，（点击数量，下单数量，支付数量））
    //join,zip,leftOuterJoin,cogroup
    val cogrouprdd: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clicCountRDD.cogroup(orderCountRDD, payCountRDD)
    val analysisRDD=cogrouprdd.mapValues{
      case (clickiter,orderiter,payiter)=> {
          var clickCnt = 0
          val iter1 = clickiter.iterator
          if (iter1.hasNext) {
            clickCnt = iter1.next()
          }

          var orderCnt = 0
          val iter2 = orderiter.iterator
          if (iter2.hasNext) {
            clickCnt = iter2.next()
          }
          var payCnt = 0
          val iter3 = payiter.iterator
          if (iter3.hasNext) {
            clickCnt = iter3.next()
          }
          (clickCnt,orderCnt,payCnt)
        }
    }
    val resultrdd: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(x => x._2, false).take(10)
    //6.将结果采集到控制台
    resultrdd.foreach(println)
    sc.stop()
  }
}
