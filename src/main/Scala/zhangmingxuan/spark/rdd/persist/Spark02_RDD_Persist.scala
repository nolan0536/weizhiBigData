package zhangmingxuan.spark.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    val list =List("hello scala","hello spark")
    val rdd: RDD[String] = sc.makeRDD(list)
    val flatrdd: RDD[String] = rdd.flatMap(x => x.split(" "))
    val maprdd: RDD[(String, Int)] = flatrdd.map(
      x => {
        println("@@@@@@@@")
        (x, 1)
    })
    //cache默认持久化的操作，只能将数据保存到内存中，如果想要保存到磁盘文件，需要更改存储级别
    //maprdd.cache()  //缓存
    maprdd.persist(StorageLevel.DISK_ONLY)
    val reducerdd: RDD[(String, Int)] = maprdd.reduceByKey(_ + _)
    reducerdd.collect().foreach(println)
    println("***********************************")
    val grouprdd: RDD[(String, Iterable[Int])] = maprdd.groupByKey()
    grouprdd.collect().foreach(println)

    sc.stop()

  }

}
