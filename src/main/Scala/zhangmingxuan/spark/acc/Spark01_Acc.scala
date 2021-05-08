package zhangmingxuan.spark.acc

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.rdd.RDD
object Spark01_Acc {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("acc")
    val sc=new SparkContext(sparkconf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //reduce:分区内的计算和分区间的计算
//    val i: Int = rdd.reduce(_ + _)
//    println(i)
    var sum=0
    val sums: Unit = rdd.foreach(
      num => {
        sum = sum + num
      }
    )
    println("sum="+sums)
    sc.stop()
  }

}
