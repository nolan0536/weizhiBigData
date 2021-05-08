package zhangmingxuan.spark.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("Action")
    val sc=new SparkContext(sf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //TODO -行动算子
    //所谓的行动算子，其实就是触发作业（job）执行的方法
    //底层代码调用的时环境对象的runJob方法/
    //底层代码中会创建ActiveJob，并提交执行

    //reduce
    val rdd1: Int = rdd.reduce((x:Int,y:Int)=>x+y)
    //  print(rdd1)

    //collect:方法会将不同分区的数据按照分区顺序采集到Driver端内存中，形成数据
    val rdd2: Array[Int] = rdd.collect()
    //println(rdd2.mkString(","))

    val rdd3: Long = rdd.count()
    //println(rdd3)
    val rdd4: Int = rdd.first()
    //println(rdd4)
    val rdd5: Array[Int] = rdd.take(3)
    //println(rdd5.mkString(","))
    val rdd6: Array[Int] = rdd.takeOrdered(3)
    //println(rdd6.mkString(","))







    sc.stop()

  }
}
