package zhangmingxuan.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    //TODO算子 map -mapPartitions  可以以分区为单位进行数据转换操作
    //但是会将整个分区的数据加载到内存进行引用 如果处理完的数据是不会被释放掉，存在对象的引用
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    // index指的是哪个分区
    val mpiRDD: RDD[Int] = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 0) {
          iter
        } else {
          Nil.iterator
        }
      }
    )
    mpiRDD.collect().foreach(println)
    sc.stop()
  }

}
