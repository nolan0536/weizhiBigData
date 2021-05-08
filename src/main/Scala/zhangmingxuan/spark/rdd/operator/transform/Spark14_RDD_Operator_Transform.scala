package zhangmingxuan.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark14_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    //TODO算子 - key——value类型

    val rdd = sc.makeRDD(List(1,2,3,4),2)
    val maprdd: RDD[(Int, Int)] = rdd.map((_, 1))
    //RDD=>PairRDDFunctions
    //隐式转换（二次编译）
    // partitionBy根据指定的分组规则对数据进行重分区
    maprdd.partitionBy(new HashPartitioner(2))
      .saveAsTextFile("outpath")


    sc.stop()
  }

}
