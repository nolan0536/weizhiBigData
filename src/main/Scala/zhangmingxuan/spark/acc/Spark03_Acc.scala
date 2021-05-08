package zhangmingxuan.spark.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Acc {
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
    val maprdd: RDD[Int] = rdd.map(
      num => {
      //使用累加器
        sum.add(num)
        num
      }
    )
    //少加：转换算子中调用累加器，如果没有行动算子的话，那么不会执行
    //多加：转换算子中调用累加器，如果没有行动算子的话，那么不会执行
    //一般情况下，累加器会放置在行动算子中进行操作
    maprdd.collect()
    //累加器是全局共享的调用一次行动算子就会走一遍
    println(sum.value)
    sc.stop()
  }

}
