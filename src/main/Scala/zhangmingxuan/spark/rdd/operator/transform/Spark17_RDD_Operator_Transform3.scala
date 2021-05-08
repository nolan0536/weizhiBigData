package zhangmingxuan.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Operator_Transform3 {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    //TODO算子 - key——value类型

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2),("b",3) ,("b", 4), ("b", 5),("a",6)
    ),2)
    val rdd1: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
      (t, v) => {
        println(t,v)
        (t._1 + v, t._2 + 1)
      },
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )
    val rdd2: RDD[(String, Int)] = rdd1.mapValues {
      case (num, cnt) => {
        num / cnt
      }
    }
    rdd2.collect().foreach(println)
    sc.stop()
  }

}
