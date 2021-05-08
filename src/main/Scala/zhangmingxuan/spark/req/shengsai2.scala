package zhangmingxuan.spark.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext,SparkConf}
object shengsai2 {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf()
      .setAppName("qingxi2")
      .setMaster("local[*]")
    val sc=new SparkContext(sparkconf)

    val rdd01: RDD[String] = sc.textFile("output/part-00000")
    val data: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String)] = rdd01.map(l => {
      val datas = l.split(",")
      try {
        if (datas(7).toDouble > -1) {
          (datas(0), datas(1), datas(2), datas(3), datas(4), datas(5), datas(6), datas(7), datas(8), datas(9), datas(10), datas(11), datas(12))
        } else {
          ("aaa1", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa")
        }
      } catch {
            //数字格式异常
        case e: NumberFormatException => {
          ("aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa")
        }
      }
    })
    val rdd5: RDD[(String, Int)] = data.map(l => (l._1, 1)).reduceByKey(_ + _)
    rdd5.collect().foreach(println)
    val rdd6: RDD[Unit] = rdd5.map {
      l => {
        if (l._1 == "aaa") {
          println("删除的行数为：" + l._2)
        }
      }
    }
    rdd6.collect()
    val data2: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String)] = data.filter(l => (l._1 != "aaa"))
    data2.map(l=>(l.productIterator.mkString(","))).coalesce(1).saveAsTextFile("output2")
    sc.stop()
  }

}
