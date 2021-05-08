package zhangmingxuan.spark.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {
    //准备环境
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("rdd")
//这样设置的话默认分区数就为五    sparkconf.set("spark.default.parallelism","5")
    val sc= new SparkContext(sparkconf)

    //创建rdd
    //rdd的并行度&分区
    //makeRDD方法可以传递第二个参数，这个参数表示分区的数量
    //第二个参数可以是不传递的，那么makeRDD方法会使用默认值：默认并行度
    //spark在默认情况下，从配置对象中获取配置参数：spark.default.parallelism
    //如果获取不到，那么使用totalCores属性，这个属性取值为当前运行环境的最大可用核数
    val seq = Seq(1,2,3,4,5)
    val rdd: RDD[Int] = sc.makeRDD(seq,2)
    //将处理的数据保存成分区文件
    rdd.saveAsTextFile("output")

    //关闭环境
    sc.stop()

  }

}
