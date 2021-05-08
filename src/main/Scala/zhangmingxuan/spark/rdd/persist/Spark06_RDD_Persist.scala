package zhangmingxuan.spark.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Persist {
  def main(args: Array[String]): Unit = {
    //cache:将数据临时存储在内存中进行数据重用
    //         会在血缘关系中添加新的依赖。一旦出现问题，可以重头读取数据

    //persist：将数据临时存储在磁盘文件中进行数据的重用
    //         涉及到磁盘io，性能较低，但是数据安全
    //         如果作业执行完毕，临时保存的数据文件就会丢失

    //checkpoint：将数据长久地保存在磁盘文件中进行数据重用
    //         涉及到磁盘io，性能较低，但是数据安全
    //         为了保证数据安全，所以一般情况下，会独立执行作业
    //         为了能够提高效率，一般情况下，是需要和cache联合使用
    //         执行过程中，会切断血缘关系，重新建立新的血缘关系
    //         checkpoint等同于改变数据源

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
        (x, 1)
    })
    //maprdd.cache()

    maprdd.checkpoint()
    println(maprdd.toDebugString)
    val reducerdd: RDD[(String, Int)] = maprdd.reduceByKey(_ + _)
    reducerdd.collect().foreach(println)
    println("***********************************")
    println(maprdd.toDebugString)

    sc.stop()

  }

}
