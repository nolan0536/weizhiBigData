package demo.analysis

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ETLSparkCore {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf()
      .setAppName("SparkCoreTest")
      .setMaster("local[2]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    case class Student(id: Int,
                       name: String,
                       sex: String)
    //读取数据
    val rdd01: RDD[String] = sc.textFile("file:///D:\\data\\student.csv")
    val data = rdd01.map(l => {
      val datas = l.split(",")
      try {
        if (datas.length < 3) {
          //字段数异常判断
          new Student(0, "a1", "a1")
        } else if ((datas(0) == null || datas(0).trim().length < 1) || (datas(1) == null || datas(1).trim().length < 1) || (datas(2) == null || datas(2).trim().length < 1)) {
          //判断字段为空，是否为空值
          new Student(0, "a2", "a2")
        } else if (datas(0).toInt < 0) {
          //异常值
          new Student(0, "a3", "a3")
        } else {
          new Student(datas(0).toInt, datas(1), datas(2))
        }
      } catch {
        case e: NumberFormatException => new Student(0, "a3", "a3") //数字类型转换异常
        case e: Exception => {
          e.printStackTrace()
          new Student(0, "a4", "a4")
        } //其他异常
      }
    })
    data.coalesce(1).foreach(println(_))

    data.map(r => (r.name,1))
      .groupByKey()
      .map(tuple => {
        var sum = 0.0
        val num = tuple._2.size
        (tuple._1, num)
      }).coalesce(1)
      .foreach(line=>
        if(line._1.toString.equals("a1")){
          println("字段数异常==="+line._2)
        }else if(line._1.toString.equals("a2")){
          println("字空字段异常==="+line._2)
        }else if(line._1.toString.equals("a3")) {
          println("数字转换异常===" + line._2)
        }else if(line._1.toString.equals("a4")) {
          println("其他异常===" + line._2)
        }
      )
//    数据清洗去重，将每行数据转换成字符逗号分隔
    val resData=data.filter(l=>l.name !="a1").filter(l=>l.name !="a2").filter(l=>l.name !="a3").filter(l=>l.name !="a4")//过滤异常值
        .distinct()//去重
      .map(line=>line.id+","+line.name+","+line.sex)//转换成指定的字符串格式
    //输出结果
    resData.coalesce(1).foreach(println(_))
    //串并保存到本地
    resData.coalesce(1).saveAsTextFile("file:///D:\\data\\sparkout5")

    sc.stop()
  }
}
