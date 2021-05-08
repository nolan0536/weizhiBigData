package zhangmingxuan.spark.rdd.operator.action

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.rdd.RDD
object Spark01_RDD_Operator_Action {
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
    rdd.collect()
    sc.stop()

  }


}
