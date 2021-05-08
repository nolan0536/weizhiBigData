package zhangmingxuan.spark.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Acc {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("acc")
    val sc=new SparkContext(sparkconf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //获取系统的累加器
    //spark默认就提供了简单数据聚合的累加器
    val sum: LongAccumulator = sc.longAccumulator("sum")
//    sc.doubleAccumulator("sum")
//    sc.collectionAccumulator("sum")
    rdd.foreach(
      num => {
        //使用累加器
        sum.add(num)
      }
    )
    println(sum.value)
    sc.stop()
  }

}
