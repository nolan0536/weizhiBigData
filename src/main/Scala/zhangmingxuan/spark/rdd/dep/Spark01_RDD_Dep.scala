package zhangmingxuan.spark.rdd.dep

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.rdd.RDD
object Spark01_RDD_Dep {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("rdd")
    val sc = new SparkContext(sparkconf)
    val lines: RDD[String] = sc.textFile("datas/word")
    println(lines.toDebugString)
    println("*******************8")
    val words:RDD[String]=lines.flatMap(line=>line.split(" "))
    println(words.toDebugString)
    println("*******************8")
    val wordtoone=words.map(word=>(word,1))
    println(wordtoone.toDebugString)
    println("*******************8")
    //spark框架提供了更多的功能，可以将分组和聚合使用一个方法实现
    //相同的key的数据，可以对value进行聚合
    val wordctoount=wordtoone.reduceByKey((x,y)=>x+y)
    println(wordctoount.toDebugString)
    println("*******************8")
    val array:Array[(String,Int)] = wordctoount.collect()
    array.foreach(println)
    sc.stop()

  }

}
