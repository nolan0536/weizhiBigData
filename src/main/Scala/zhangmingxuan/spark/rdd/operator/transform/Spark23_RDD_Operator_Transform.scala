package zhangmingxuan.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark23_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("b", 1) ,("c", 3)
    ))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 4), ("b", 5), ("c", 6),("c",5)
    ))

    //cogroup:connect+group(分组，连接)
    val rdd3: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)


    rdd3.collect().foreach(println)
    //rdd4.collect().foreach(println)
    sc.stop() 
  }

}
