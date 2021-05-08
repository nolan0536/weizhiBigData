package zhangmingxuan.spark.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Req1_HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    //TODO:top10热门品类
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("shixun")
    val sc=new SparkContext(sparkconf)
    //问题1：rdd重复使用次数多
    //问题2：cogroup性能较低
    //1.读取原始日志数据
    val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    rdd.cache()

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
    //5.
    //思路：
    //(品类ID，点击数量)=>(品类ID，（点击数量，0，0）)
    //(品类ID，下单数量)=>(品类ID，（0，下单数量，0）)
    //               =>(品类ID，（点击数量，下单数量，0）)
    //(品类ID，支付数量)=>(品类ID，（0，0，支付数量）)
    //               =>(品类ID，（点击数量，下单数量，支付数量）)
   val rdd1=clicCountRDD.map{
     case (cid,cnt)=>{
       (cid,(cnt,0,0))
     }
   }
    val rdd2=orderCountRDD.map{
      case (cid,cnt)=>{
        (cid,(0,cnt,0))
      }
    }
    val rdd3=payCountRDD.map{
      case (cid,cnt)=>{
        (cid,(0,0,cnt))
      }
    }
    //将三个数据源合并在一起，统一进行聚合计算
    val soruceRDD: RDD[(String, (Int, Int, Int))] = rdd1.union(rdd2).union(rdd3)
    val analysisRDD: RDD[(String, (Int, Int, Int))] = soruceRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )
    val resultrdd: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(x => x._2, false).take(10)
    //6.将结果采集到控制台
    resultrdd.foreach(println)
    sc.stop()
  }
}
