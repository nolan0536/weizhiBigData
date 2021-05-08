package zhangmingxuan.spark.req

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.rdd.RDD
object shengsai3 {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("qingxi1")
    val sc=new SparkContext(sparkconf)
    val rdd: RDD[String] = sc.textFile("datas/data.csv")
    val data2: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String)] = rdd.map {
      l => {
        val datas: Array[String] = l.split(",")
        if (datas.size == 13) {
          if (((datas(8).substring(1,datas(8).size-1)==" ")||(datas(8).substring(1,datas(8).size-1)=="0"))&&((datas(9).substring(1,datas(9).size-1)!=" ")||((datas(9).substring(1,datas(9).size-1))!="0"))){
            ("aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa")
          } else if ((datas(0).substring(0, 1) == "\"") && (datas(0).substring(datas(0).size - 1, datas(0).size) == "\"")) {
            (datas(0).substring(1, datas(0).size - 1), datas(1).substring(1, datas(1).size - 1),
              datas(2).substring(1, datas(2).size - 1), datas(3).substring(1, datas(3).size - 1),
              datas(4).substring(1, datas(4).size - 1), datas(5).substring(1, datas(5).size - 1),
              datas(6).substring(1, datas(6).size - 1), datas(7).substring(1, datas(7).size - 1),
              datas(8).substring(1, datas(8).size - 1), datas(9).substring(1, datas(9).size - 1),
              datas(10).substring(1, datas(10).size - 1), datas(11).substring(1, datas(11).size - 1),
              datas(12).substring(1, datas(12).size - 1))
          } else {
            ("aaa1", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa")
          }
        } else {
          ("aaa2", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa")
        }
      }
    }
    val rdd3: RDD[(String, Int)] = data2.map {
      l => {
        (l._1, 1)
      }
    }.reduceByKey(_ + _)
    rdd3.map{
      l=>{
        if(l._1=="aaa"){
          println("删除的行数为："+l._2)
        }
      }
    }.collect()
    val data3: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String)] = data2.filter(l => (l._1 != "aaa")).filter(l => (l._1 != "aaa1")).filter(l => (l._1 != "aaa2"))
    data3.map(l=>(l.productIterator.mkString(","))).coalesce(1).saveAsTextFile("output3")
    sc.stop()
  }

}
