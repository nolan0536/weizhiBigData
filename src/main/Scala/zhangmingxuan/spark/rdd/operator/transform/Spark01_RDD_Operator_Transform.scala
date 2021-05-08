package zhangmingxuan.spark.rdd.operator.transform

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.rdd.RDD
object Spark01_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    //TODO算子 map
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))

    //转换函数
    def mapFunction(num:Int):Int={
      num*2
    }
    //匿名函数
    val rdd2:RDD[Int]=rdd.map(x=>x*2)
    val rdd3=rdd2.collect()
    rdd3.foreach(println)
    sc.stop()
  }

}
