package zhangmingxuan.spark.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SPARK_BRANCH, SparkConf, SparkContext}

object Spark06_Req3_HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    //TODO:top10热门品类
    val sparkconf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("shixun")
    val sc = new SparkContext(sparkconf)

    val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    val actionDataRDD: RDD[UserVisitAction] = rdd.map {
      action => {
        val datas: Array[String] = action.split("_")
        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong
        )
      }
    }
    //TODO对指定的页面连续跳转进行统计
    val ids =List[Long](1,2,3,4,5,6,7)
    val okflowIds: List[(Long, Long)] = ids.zip(ids.tail)
    //1.计算分母
    actionDataRDD.cache()
    val actionDataRDD2: RDD[UserVisitAction] = actionDataRDD.filter(
      action => {
        ids.contains(action.page_id)
      }
    )
    val pageidToCountMap: Map[Long, Int] = actionDataRDD2.map(
      action => {
        (action.page_id, 1)
      }
    ).reduceByKey(_ + _).collect().toMap
    //2.计算分子
    val sessionid: RDD[(String, Iterable[UserVisitAction])] = actionDataRDD.groupBy(_.session_id)
    //分组后，根据访问时间进行排序（升序）
    val mvRDD: RDD[(String, List[((Long, Long), Int)])] = sessionid.mapValues(
      iter => {
        val sortrdd: List[UserVisitAction] = iter.toList.sortBy(x => x.action_time)
        val flowids: List[Long] = sortrdd.map(x => x.page_id)
        val pageflowids: List[(Long, Long)] = flowids.zip(flowids.tail) //前一个和后一个放一块
        //将不合法的页面跳转进行过滤
        pageflowids.filter(
          t=>{
            okflowIds.contains(t)
          }
        ).map {
          t => {
            (t, 1)
          }
        }
      }
    )
    val flatRDD: RDD[((Long, Long), Int)] = mvRDD.map(_._2).flatMap(list => list)
    val dataRDD: RDD[((Long, Long), Int)] = flatRDD.reduceByKey(_ + _)
    //TOTD计算单跳转换率
    //分子除以分母
    dataRDD.foreach{
      case ((pageid1,pageid2),sum)=>{
        val lon: Int = pageidToCountMap.getOrElse(pageid1, 0)
        println(s"页面${pageid1}跳转到页面${pageid2}单跳转换率为:"+(sum.toDouble/lon))
      }
    }
    sc.stop()
  }
  //用户访问动作表
  case class UserVisitAction(
                              date: String,//用户点击行为的日期
                              user_id: Long,// 用 户 的 ID
                              session_id: String,//Session 的 ID
                              page_id: Long,// 某 个 页 面 的 ID
                              action_time: String,//动作的时间点
                              search_keyword: String,//用户搜索的关键词
                              click_category_id: Long,// 某 一 个 商 品 品 类 的 ID
                              click_product_id: Long,// 某 一 个 商 品 的 ID
                              order_category_ids: String,//一次订单中所有品类的 ID 集合
                              order_product_ids: String,//一次订单中所有商品的 ID 集合
                              pay_category_ids: String,//一次支付中所有品类的 ID 集合
                              pay_product_ids: String,//一次支付中所有商品的 ID 集
                              city_id: Long
                            )//城市 id
}
