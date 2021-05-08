package zhangmingxuan.spark.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("Action")
    val sc=new SparkContext(sf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val user=new User()
    //RDD算子中传递的函数是会包含闭包操作，那么就会进行检测功能
    //闭包检测功能
    rdd.foreach(
      num => {
        println("age=" + (user.age + num))
      }
    )

    sc.stop()
  }
  //必须序列化
  //class User extends Serializable
  //样例类在编译时，会自动混入序列化特质（实现可序列化接口）
  case class User(){
    var age :Int = 30
  }
}
