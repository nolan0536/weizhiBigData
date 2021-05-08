package zhangmingxuan.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark15_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    //TODO算子 - key——value类型

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("b", 4)
    ))
    //reduceByKey:相同的key的数据进行value数据的聚合操作
    //scala语言中一般的聚合操作都是两两聚合，spark基于Scala进行开发，所以也是两两聚合
    //
    val reducerdd: RDD[(String, Int)] = rdd.reduceByKey((x: Int, y: Int) => {
      println(s"x=${x},y=${y}")
      x + y
    })

    reducerdd.collect().foreach(println)
    sc.stop()
  }

}
