package demo.clean

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object clean1 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf()
      .setAppName("SparkCoreTest")
      .setMaster("local[2]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //  所属年月,商家名称,主营类型,店铺URL,特色菜,累计评论数,累计销售量,店铺评分,本月销量,本月销售额,城市,商家地址,电话
    //       0        1       2       3     4       5         6         7         8         9    10  11      12

    //读取数据
    val rdd01: RDD[String] = sc.textFile("file:///D:\\data\\org\\data.csv")
//val rdd01: RDD[String] = sc.textFile(args(0))
    val data = rdd01.map(l => {
      val datas = l.split(",")
      if (datas.size == 13) {
        if ((datas(8) == null || datas(8) == "0") && (datas(9) == null || datas(9) == "0")) {
          ("aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa")
        } else if (datas(3).substring(0, 1) == "\"" && datas(3).substring(datas(3).size - 1) == "\"") {
          (datas(0), datas(1), datas(2), datas(3).substring(1, datas(3).size - 1), datas(4), datas(5), datas(6), datas(7), datas(8), datas(9), datas(10), datas(11), datas(12))
        } else {
          ("aaa1", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa")
        }
      } else {
        ("aaa2", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa")
      }
    })
    data.coalesce(1).foreach(println(_))
    data.map(l => (l._1, 1))
      .groupByKey()
      .map(t => {
        (t._1, t._2.size)
      }
      ).coalesce(1)
      .foreach(re => {
        if (re._1 == "aaa") {
          println(".....删除的条目数为：" + re._2 + "行......")
        }
      })
    //过滤异常值
    val resData = data.filter(l => l._1 != "aaa").filter(l => l._1 != "aaa1").filter(l => l._1 != "aaa2")
    //将tuple转换成字符串
    resData.map(l => {
      l.productIterator.mkString(",")
    })
    //串并保存到本地
        .coalesce(1).saveAsTextFile("file:///D:\\data\\bisai\\coreClean12321")
//      .coalesce(1).saveAsTextFile(args(1))
    sc.stop()
  }
}
