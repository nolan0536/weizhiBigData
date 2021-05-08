package zhangmingxuan.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {

    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    //TODO算子 - sortBy
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("1", 1), ("11", 2), ("2", 3)), 2)
    //sortBy方法可以根据指定的规则对数据源中的数据进行爱旭，默认为升序，第二个参数可以改变排序的方式
    //sortBy默认情况下，不会改变分区，但是中间存在shuffle操作
    val newRDD: RDD[(String, Int)] = rdd.sortBy(t => t._1.toInt,false)
    newRDD.collect().foreach(println)
    newRDD.saveAsTextFile("outpath")
    sc.stop()
  }

}
