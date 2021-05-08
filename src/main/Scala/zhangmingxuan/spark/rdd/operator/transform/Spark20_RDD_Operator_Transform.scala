package zhangmingxuan.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark20_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    //TODO算子 - key——value类型

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2),("b",3) ,("b", 4), ("b", 5),("a",6)
    ),2)
    rdd.reduceByKey(_+_)
    rdd.aggregateByKey(0)(_+_,_+_)
    rdd.foldByKey(0)(_+_)
    rdd.combineByKey(v=>v,(x:Int,y)=>x+y,(x:Int,y:Int)=>x+y)

    sc.stop()
  }

}
