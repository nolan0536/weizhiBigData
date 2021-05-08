package zhangmingxuan.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Transform_test {
  def main(args: Array[String]): Unit = {

    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    //TODO算子 map -mapPartitions  可以以分区为单位进行数据转换操作
    //但是会将整个分区的数据加载到内存进行引用 如果处理完的数据是不会被释放掉，存在对象的引用
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)

    //总共执行两次 每个分区走一次
    //mapPartitions每次执行一个分区的数据
    //获取每个数据分区的最大值
    //使用iterator把列表里的最大值转换成迭代器
    val mprdd: RDD[Int] = rdd.mapPartitions(
      iter => {
        List(iter.max).iterator
      }
    )
    mprdd.collect().foreach(println)

    sc.stop()
  }

}
