package zhangmingxuan.spark.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    sc.setCheckpointDir("cp")
    val list =List("hello scala","hello spark")
    val rdd: RDD[String] = sc.makeRDD(list)
    val flatrdd: RDD[String] = rdd.flatMap(x => x.split(" "))
    val maprdd: RDD[(String, Int)] = flatrdd.map(
      x => {
        println("@@@@@@@@")
        (x, 1)
    })
    //checkpoint 需要落盘，需要指定检查点保存路径
    //检查点路径保存的文件，当作业执行完毕后，不会被删除
    //一般保存路径都在分布式储存系统：hdfs
    maprdd.checkpoint()
    val reducerdd: RDD[(String, Int)] = maprdd.reduceByKey(_ + _)
    reducerdd.collect().foreach(println)
    println("***********************************")
    val grouprdd: RDD[(String, Iterable[Int])] = maprdd.groupByKey()
    grouprdd.collect().foreach(println)

    sc.stop()

  }

}
