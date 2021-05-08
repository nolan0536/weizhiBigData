//package acc
//
//import org.apache.spark.rdd.RDD
//import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
//import org.apache.spark.{SparkConf, SparkContext}
//import scala.collection.mutable
//
//object Spark04_Acc_WordCount {
//  def main(args: Array[String]): Unit = {
//    val sparkconf = new SparkConf()
//      .setMaster("local[*]")
//      .setAppName("acc")
//    val sc = new SparkContext(sparkconf)
//    val rdd: RDD[String] = sc.makeRDD(List("hello", "spark", "hello"))
//
//    //创建累加器对象
//    val wcACC = new MyAccmulator()
//    //向spark进行注册
//    sc.register(wcACC, "wordCountAcc")
//    rdd.foreach(
//      word => {
//        //数据的累加 （使用累加器）
//        wcACC.add(word)
//      }
//    )
//    //获取累加的结果
//    println(wcACC.value)
//    sc.stop()
//  }
//
//  //自定义累加器
//  /*
//  1.继承AccumulatorV2,定义泛型
//    IN：累加器输入的数据类型
//    OUT：累加器返回的数据类型 mutable.Map[String,Long]
//  2.重写方法
//   */
//  class MyAccmulator extends AccumulatorV2[String, mutable.Map[String, Long]] {
//    private var wcMap = mutable.Map[String, Long]()
//
//    //判断是否初始状态
//    override def isZero: Boolean = {
//      wcMap.isEmpty
//    }
//
//    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
//      new MyAccmulator()
//    }
//
//    override def reset(): Unit = {
//      wcMap.clear()
//    }
//
//    //获取累加器需要计算的值
//    override def add(word: String): Unit = {
//      val newCnt = wcMap.getOrElse(word, ol) + 1
//      wcMap.update(word, newCnt)
//    }
//
//    //Driver合并多个累加器
//    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
//      val map1 = this.wcMap
//      val map2 = other.value
//      map2.foreach {
//        case (word, count) => {
//          val newCount = map1.getOrElse(word, ol) + count
//          map1.update(word, newCount)
//        }
//      }
//    }
//      //累加器结果
//    override def value: mutable.Map[String, Long] = {
//        wcMap
//    }
//
//  }
//
//}