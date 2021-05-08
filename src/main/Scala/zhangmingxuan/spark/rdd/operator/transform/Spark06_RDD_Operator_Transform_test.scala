package zhangmingxuan.spark.rdd.operator.transform

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform_test {
  def main(args: Array[String]): Unit = {

    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    //TODO算子 - groupby
    val rdd: RDD[String] = sc.textFile("datas/apache.log")
    val timeRDD: RDD[(String, Iterable[(String, Int)])] = rdd.map(
      x => {
        val datas = x.split(" ")
        val time = datas(3)
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val data: Date = sdf.parse(time)
        val sdf1 = new SimpleDateFormat("HH")
        val hour: String = sdf1.format(data)
        (hour, 1)
      }
    ).groupBy(_._1)
    timeRDD.map{
      case (hour,iter)=>{
        (hour,iter.size)
      }
    }.collect().foreach(println)
    sc.stop()
  }

}
