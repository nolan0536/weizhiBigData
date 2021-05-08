package zhangmingxuan.spark.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object Spark01_RDD_Serial {
  def main(args: Array[String]): Unit = {
    val sf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("Action")
    val sc=new SparkContext(sf)
    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))
    val search=new Search("h")
    search.getMatch2(rdd).collect().foreach(println)
    sc.stop()
  }
  //查询对象
  //类的构造参数其实就是类的属性，构造参数需要进行闭包检测，其实就等同于类进行闭包检测
  class Search(query:String)extends Serializable{
    def isMatch(s: String): Boolean = {
      //判断字符串是否有子字符串 有返回true没有返回false
      s.contains(query)
    }

    // 函数序列化案例
    def getMatch1 (rdd: RDD[String]): RDD[String] = {
      rdd.filter(isMatch)
    }
    // 属性序列化案例
    def getMatch2(rdd: RDD[String]): RDD[String] = {
      rdd.filter(x => x.contains(query))

    }
  }
}
